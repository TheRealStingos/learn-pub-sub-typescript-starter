import amqp from "amqplib";
import { ExchangePerilDirect } from "../internal/routing/routing.js";
import { PauseKey } from "../internal/routing/routing.js";
import { type PlayingState } from "../internal/gamelogic/gamestate.js";

async function main() {
  console.log("Starting Peril server...");
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  conn.createConfirmChannel();
  console.log("Connected to RabbitMQ");
  process.on("SIGINT", () => {
    console.log("Closing RabbitMQ connection...");
    conn.close();
    process.exit(0);
  })
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
