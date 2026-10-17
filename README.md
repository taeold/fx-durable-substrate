# fx-durable-substrate

A simple durable AI coding agent built with [libfx](https://fx.sh/docs/lib) running on [Agent Substrate](https://github.com/agent-substrate/substrate).

## agent/server.ts

```ts
import http from "node:http";
import { readdir } from "node:fs/promises";
import { Readable } from "node:stream";
import { json } from "node:stream/consumers";
import { createFxAgent } from "libfx";
import { local } from "libfx/durable-local";
import { createCodingTools } from "./tools.ts";

const agent = createFxAgent({
  model: "google/gemini-3.8-flash",
  durability: local({ dir: "/workspace/.fx/sessions" }),
  tools: createCodingTools("/workspace"),
});

// Get-or-create the actor's session:
// - On initial actor boot, runs/ is empty, so agent.session(undefined) creates a new session.
// - On resume/fork, readdir finds the existing "wrun_<ulid>.json" and reopens that exact session.
const [existingRun] = await readdir("/workspace/.fx/sessions/runs").catch(() => []);
const sessionId = existingRun ? existingRun.replace(/\.json$/, "") : undefined;
const session = agent.session(sessionId);
await session.resume().result;

const server = http.createServer(async (req, res) => {
  if (req.url === "/readyz") return res.end("ok\n");
  if (req.method === "GET" && req.url?.startsWith("/events")) {
    const cursor = Number(new URL(req.url, "http://localhost").searchParams.get("cursor") ?? 0);
    res.writeHead(200, { "Content-Type": "application/x-ndjson" });
    const stream = Readable.fromWeb(session.stream(cursor));
    req.on("close", () => stream.destroy());
    return stream.pipe(res);
  }
  const { prompt } = (await json(req)) as { prompt: string };
  res.writeHead(200, { "Content-Type": "application/x-ndjson" });
  Readable.fromWeb(session.prompt(prompt).readable).pipe(res);
});

process.on("SIGTERM", async () => {
  await agent.close();
  server.close(() => process.exit(0));
});
server.listen(80, "0.0.0.0");
```

## How it works

`libfx` runs the coding agent loop and persists every conversation turn, tool call step, and stream chunk to `/workspace/.fx/sessions` via `local({ dir })`.

Agent Substrate runs each agent as an isolated actor with `/workspace` mounted as a durable directory (`durableDir` in `template.yaml`).

1. **Suspend and auto-resume**: When you suspend an actor (`kubectl ate suspend actor`), Agent Substrate sends `SIGTERM` so `libfx` releases its session lease (`await agent.close()`), snapshots `/workspace` to object storage, and stops the container. When the next HTTP request arrives for that actor, the router restores `/workspace` onto a worker pod and wakes the server back up before forwarding the request. On local kind, snapshots are stored in the bundled in-cluster S3 store (`rustfs`); on GKE, `atelet` and `ate-api-server` write snapshots to GCS using Workload Identity without any storage credentials in the actor.
2. **Actor-scoped session lifecycle (get-or-create)**: In `local({ dir })` mode, `libfx` stores sessions as `runs/wrun_<ulid>.json`. On initial boot, `runs/` is empty so `agent.session(undefined)` creates a fresh session. On wake or fork, `existingRun` automatically finds and reopens the snapshot's existing run (`agent.session(sessionId)`), ensuring the session identity travels with the volume snapshot across forks without callers having to manage session IDs.
3. **Reconnectable event streaming**: `POST /submit` streams newline-delimited JSON (`turn.readable`) as the turn runs, stamping every event with a `cursor`. `GET /events?cursor=N` (`session.stream(cursor)`) replays or tails the session's durable event log from any cursor—even across actor suspends, restarts, or forks.
4. **Checkpoint and fork**: Because both the agent session history (`.fx/sessions`) and the working files live in `/workspace`, you can tag a suspended actor (`kubectl ate create tag`) and create a new actor from that tag (`kubectl ate create actor --tag`). Both actors start from the same history and files, then diverge on subsequent requests.
5. **Egress credential injection**: The sandbox runs with a placeholder `AI_GATEWAY_API_KEY`. Outbound calls to `ai-gateway.vercel.sh` pass through the Agent Substrate egress proxy, which injects the real `Authorization` header from `egress-policy.yaml` so secrets never enter the sandbox or its snapshots.

## Quickstart (local Kubernetes with kind)

### 1. Start Agent Substrate on a local kind cluster

Make sure you have Go, Docker, and `kubectl` installed, then spin up the local cluster, publish the worker runtime image, and install `kubectl-ate`:

```bash
git clone https://github.com/agent-substrate/substrate.git
cd substrate
hack/create-kind-cluster.sh
hack/install-ate-kind.sh --deploy-ate-system --credential-provider='{"name":"k8s.io"}'
go run ./cmd/ate-setup --kind publish worker-images
go install ./cmd/kubectl-ate
cd ..
```

### 2. Build and deploy the agent template

Set your `AI_GATEWAY_API_KEY`, store it in a Kubernetes Secret for the egress proxy, and deploy the worker pool and actor template:

```bash
export AI_GATEWAY_API_KEY="your-ai-gateway-api-key"

# Build and push the agent image to the local kind registry
docker build -t localhost:5001/fx-durable-actor:latest ./agent
docker push localhost:5001/fx-durable-actor:latest
DIGEST=$(docker inspect --format='{{index .RepoDigests 0}}' localhost:5001/fx-durable-actor:latest)
sed -i "s|image: .*|image: ${DIGEST}|" template.yaml

# Deploy the worker pool
kubectl apply -f workerpool.yaml
kubectl -n ate-demo-fx rollout status deployment/fx-durable

# Create the atespace and actor template
kubectl ate create atespace ate-demo-fx
kubectl ate create actor-template -f template.yaml
kubectl ate get actor-templates -a ate-demo-fx

# Create the AI Gateway API key secret and allow ate-demo-fx to use it at the egress proxy
kubectl -n ate-demo-fx create secret generic llm-credentials \
  --from-literal=ai-gateway-api-key="Bearer ${AI_GATEWAY_API_KEY}"
kubectl -n ate-system patch configmap k8s-credential-provider-namespace-policy \
  --type merge -p '{"data":{"namespace-policy.yaml":"policies:\n- atespace: ate-demo-fx\n  allowedNamespaces:\n  - ate-demo-fx\n"}}'
kubectl -n ate-system rollout restart deployment/k8s-credential-provider

# Port-forward the router
kubectl port-forward -n ate-system svc/atenet-router 8000:80 &
```

### 3. Create an actor and stream Turn 1

```bash
kubectl ate create actor demo-agent --template fx-durable-data -a ate-demo-fx
kubectl ate create egress-policy demo-agent -a ate-demo-fx -f egress-policy.yaml

curl -N -X POST http://localhost:8000/submit \
  -H "ate-target-actor: ate-demo-fx/demo-agent" \
  -H "Content-Type: application/json" \
  -d '{"prompt": "Create app.js with add(a, b) and a test.js that verifies it."}'
```

Streamed NDJSON output:

```json
{"type":"turn_start","messageId":"msg_c390da34c67244f0a8ccbc54b3ab89d1","sessionId":"wrun_01M55DDV7TBB9FE8JGT4R9H1Q7","input":"Create app.js with add(a, b) and a test.js that verifies it.","epoch":1,"cursor":2}
{"type":"tool_start","id":"call_write_app","name":"write","input":{"path":"app.js","content":"export function add(a, b) {\n  return a + b;\n}\n"},"epoch":1,"cursor":3}
{"type":"tool_end","id":"call_write_app","name":"write","content":"{\"success\":true,\"path\":\"app.js\"}","isError":false,"epoch":1,"cursor":4}
{"type":"tool_start","id":"call_write_test","name":"write","input":{"path":"test.js","content":"import assert from 'node:assert/strict';\nimport { add } from './app.js';\nassert.equal(add(2, 3), 5);\nconsole.log('ok');\n"},"epoch":1,"cursor":5}
{"type":"tool_end","id":"call_write_test","name":"write","content":"{\"success\":true,\"path\":\"test.js\"}","isError":false,"epoch":1,"cursor":6}
{"type":"text_delta","delta":"Created `app.js` with `add(a, b)` and verified with `test.js`.","epoch":1,"cursor":7}
{"type":"turn_end","messageId":"msg_c390da34c67244f0a8ccbc54b3ab89d1","stopReason":"end_turn","usage":{},"epoch":1,"cursor":8}
```

### 4. Suspend, tag a checkpoint, and fork

```bash
# Suspend the actor (flushes libfx session store and snapshots /workspace to object storage)
kubectl ate suspend actor demo-agent -a ate-demo-fx

# Tag the snapshot and fork a second actor from that checkpoint
kubectl ate create tag checkpoint-v1 --actor demo-agent -a ate-demo-fx
kubectl ate create actor demo-agent-fork --template fx-durable-data --tag checkpoint-v1 -a ate-demo-fx
kubectl ate create egress-policy demo-agent-fork -a ate-demo-fx -f egress-policy.yaml
```

### 5. Send divergent prompts to both branches

Both actors auto-resume on demand from the shared Turn 1 history (`cursor: 1..8`) and diverge on Turn 2 (`cursor: 10..14`, after `session.resume()` emits `idle` at `cursor: 9`):

```bash
# Original actor continues on Branch A
curl -N -X POST http://localhost:8000/submit \
  -H "ate-target-actor: ate-demo-fx/demo-agent" \
  -H "Content-Type: application/json" \
  -d '{"prompt": "Add multiply(a, b) and structured logging to app.js."}'

# Forked actor continues on Branch B
curl -N -X POST http://localhost:8000/submit \
  -H "ate-target-actor: ate-demo-fx/demo-agent-fork" \
  -H "Content-Type: application/json" \
  -d '{"prompt": "Add divide(a, b) and edge-case tests to test.js."}'
```

Streamed output from `demo-agent` (Branch A):

```json
{"type":"turn_start","messageId":"msg_9c31f4a0db0e4fc093385f80631214c5","sessionId":"wrun_01M55DDV7TBB9FE8JGT4R9H1Q7","input":"Add multiply(a, b) and structured logging to app.js.","epoch":2,"cursor":10}
{"type":"tool_start","id":"call_edit_multiply","name":"edit","input":{"path":"app.js","oldText":"return a + b;\n}","newText":"return a + b;\n}\n\nexport function multiply(a, b) {\n  return a * b;\n}\n"},"epoch":2,"cursor":11}
{"type":"tool_end","id":"call_edit_multiply","name":"edit","content":"{\"success\":true,\"path\":\"app.js\"}","isError":false,"epoch":2,"cursor":12}
{"type":"text_delta","delta":"Added `multiply(a, b)` to `app.js`.","epoch":2,"cursor":13}
{"type":"turn_end","messageId":"msg_9c31f4a0db0e4fc093385f80631214c5","stopReason":"end_turn","usage":{},"epoch":2,"cursor":14}
```

Streamed output from `demo-agent-fork` (Branch B):

```json
{"type":"turn_start","messageId":"msg_88d363c6c14a4e4384396f13c334cc42","sessionId":"wrun_01M55DDV7TBB9FE8JGT4R9H1Q7","input":"Add divide(a, b) and edge-case tests to test.js.","epoch":2,"cursor":10}
{"type":"tool_start","id":"call_edit_divide","name":"edit","input":{"path":"app.js","oldText":"return a + b;\n}","newText":"return a + b;\n}\n\nexport function divide(a, b) {\n  if (b === 0) throw new Error('div by zero');\n  return a / b;\n}\n"},"epoch":2,"cursor":11}
{"type":"tool_end","id":"call_edit_divide","name":"edit","content":"{\"success\":true,\"path\":\"app.js\"}","isError":false,"epoch":2,"cursor":12}
{"type":"text_delta","delta":"Added `divide(a, b)` with error check to `app.js`.","epoch":2,"cursor":13}
{"type":"turn_end","messageId":"msg_88d363c6c14a4e4384396f13c334cc42","stopReason":"end_turn","usage":{},"epoch":2,"cursor":14}
```

You can also replay or tail either actor's event stream from any cursor (even waking a suspended actor on demand):

```bash
curl -N "http://localhost:8000/events?cursor=9" \
  -H "ate-target-actor: ate-demo-fx/demo-agent-fork"
```
