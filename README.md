# fx-durable-substrate

A simple durable AI coding agent built with [libfx](https://fx.sh/docs/lib) running on [Agent Substrate](https://github.com/agent-substrate/substrate).

## agent/server.ts

```ts
import http from "node:http";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { json } from "node:stream/consumers";
import { createFxAgent } from "libfx";
import { local } from "libfx/durable-local";
import { createCodingTools } from "./tools.ts";

await mkdir("/workspace/.fx", { recursive: true });

const agent = createFxAgent({
  model: "google/gemini-3.8-flash",
  durability: local({ dir: "/workspace/.fx/sessions" }),
  tools: createCodingTools("/workspace"),
});

let sessionId: string;
try {
  sessionId = (await readFile("/workspace/.fx/session-id", "utf8")).trim();
} catch {
  sessionId = await agent.newSessionId();
  await writeFile("/workspace/.fx/session-id", sessionId, "utf8");
}
await agent.session(sessionId).resume().result;

const server = http.createServer(async (req, res) => {
  if (req.url === "/readyz") return res.end("ok\n");
  const { prompt } = (await json(req)) as { prompt: string };
  const turn = agent.session(sessionId).prompt(prompt);
  const events = await Array.fromAsync(turn);
  const settled = await turn.result;
  res.writeHead(200, { "Content-Type": "application/json" }).end(JSON.stringify({ sessionId, settled, events }));
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
2. **Checkpoint and fork**: Because both the agent session history (`.fx/sessions`) and the working files live in `/workspace`, you can tag a suspended actor (`kubectl ate create tag`) and create a new actor from that tag (`kubectl ate create actor --tag`). Both actors start from the same history and files, then diverge on subsequent requests.
3. **Egress credential injection**: The sandbox runs with a placeholder `AI_GATEWAY_API_KEY`. Outbound calls to `ai-gateway.vercel.sh` pass through the Agent Substrate egress proxy, which injects the real `Authorization` header from `egress-policy.yaml` so secrets never enter the sandbox or its snapshots.

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

### 3. Create an actor and run Turn 1

```bash
kubectl ate create actor demo-agent --template fx-durable-data -a ate-demo-fx
kubectl ate create egress-policy demo-agent -a ate-demo-fx -f egress-policy.yaml

curl -X POST http://localhost:8000/submit \
  -H "ate-target-actor: ate-demo-fx/demo-agent" \
  -H "Content-Type: application/json" \
  -d '{"prompt": "Create app.js with add(a, b) and a test.js that verifies it."}'
```

Response snippet:

```json
{
  "sessionId": "wrun_01M55DDV7TBB9FE8JGT4R9H1Q7",
  "settled": {
    "messageId": "msg_c390da34c67244f0a8ccbc54b3ab89d1",
    "stopReason": "end_turn",
    "usage": {},
    "epoch": 1
  },
  "events": [
    {
      "type": "tool_start",
      "id": "call_write_app",
      "name": "write",
      "input": { "path": "app.js", "content": "export function add(a, b) {\n  return a + b;\n}\n" },
      "epoch": 1
    },
    {
      "type": "tool_end",
      "id": "call_write_app",
      "name": "write",
      "content": "{\"success\":true,\"path\":\"app.js\"}",
      "isError": false,
      "epoch": 1
    },
    {
      "type": "tool_start",
      "id": "call_write_test",
      "name": "write",
      "input": { "path": "test.js", "content": "import assert from 'node:assert/strict';\nimport { add } from './app.js';\nassert.equal(add(2, 3), 5);\nconsole.log('ok');\n" },
      "epoch": 1
    },
    {
      "type": "tool_end",
      "id": "call_write_test",
      "name": "write",
      "content": "{\"success\":true,\"path\":\"test.js\"}",
      "isError": false,
      "epoch": 1
    },
    {
      "type": "text_delta",
      "delta": "Created `app.js` with `add(a, b)` and verified with `test.js`.",
      "epoch": 1
    }
  ]
}
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

Both actors auto-resume on demand from the shared Turn 1 history (`epoch: 1`) and diverge on Turn 2 (`epoch: 2`):

```bash
# Original actor continues on Branch A
curl -X POST http://localhost:8000/submit \
  -H "ate-target-actor: ate-demo-fx/demo-agent" \
  -H "Content-Type: application/json" \
  -d '{"prompt": "Add multiply(a, b) and structured logging to app.js."}'

# Forked actor continues on Branch B
curl -X POST http://localhost:8000/submit \
  -H "ate-target-actor: ate-demo-fx/demo-agent-fork" \
  -H "Content-Type: application/json" \
  -d '{"prompt": "Add divide(a, b) and edge-case tests to test.js."}'
```

Response snippet from `demo-agent` (Branch A):

```json
{
  "sessionId": "wrun_01M55DDV7TBB9FE8JGT4R9H1Q7",
  "settled": { "stopReason": "end_turn", "epoch": 2 },
  "events": [
    {
      "type": "tool_start",
      "name": "edit",
      "input": {
        "path": "app.js",
        "oldText": "return a + b;\n}",
        "newText": "return a + b;\n}\n\nexport function multiply(a, b) {\n  return a * b;\n}\n"
      },
      "epoch": 2
    },
    { "type": "text_delta", "delta": "Added `multiply(a, b)` to `app.js`.", "epoch": 2 }
  ]
}
```

Response snippet from `demo-agent-fork` (Branch B):

```json
{
  "sessionId": "wrun_01M55DDV7TBB9FE8JGT4R9H1Q7",
  "settled": { "stopReason": "end_turn", "epoch": 2 },
  "events": [
    {
      "type": "tool_start",
      "name": "edit",
      "input": {
        "path": "app.js",
        "oldText": "return a + b;\n}",
        "newText": "return a + b;\n}\n\nexport function divide(a, b) {\n  if (b === 0) throw new Error('div by zero');\n  return a / b;\n}\n"
      },
      "epoch": 2
    },
    { "type": "text_delta", "delta": "Added `divide(a, b)` with error check to `app.js`.", "epoch": 2 }
  ]
}
```
