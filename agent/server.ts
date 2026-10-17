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
