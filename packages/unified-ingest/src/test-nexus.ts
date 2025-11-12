/**
 * Simple test script to connect to Nexus WebSocket and log events
 * This is Step 3 of the migration plan
 */
import WebSocket from "ws";

const ws = new WebSocket(
  process.env.NEXUS_URL ?? "ws://localhost:8080/channel",
);

ws.on("open", () => {
  console.log("Connected to Nexus");
});

ws.on("message", (data: Buffer) => {
  const event = JSON.parse(data.toString());
  if (
    event.type === "record" &&
    event.record?.collection === "app.bsky.feed.post"
  ) {
    console.log("Post event:", {
      id: event.id,
      did: event.record.did,
      rkey: event.record.rkey,
      live: event.record.live,
    });

    // Send ack
    ws.send(JSON.stringify({ id: event.id }));
  }
});

ws.on("error", (err) => {
  console.error("WebSocket error:", err);
});

ws.on("close", () => {
  console.log("WebSocket closed");
  process.exit(0);
});
