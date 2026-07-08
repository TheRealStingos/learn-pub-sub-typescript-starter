import amqp from "amqplib";
import { ExchangePerilDirect, ExchangePerilTopic } from "../internal/routing/routing.js";
import { PauseKey } from "../internal/routing/routing.js";
import { type PlayingState } from "../internal/gamelogic/gamestate.js";
import { publishJSON } from "../internal/pubsub/publish.js";
import { getInput, printServerHelp } from "../internal/gamelogic/gamelogic.js";
import { declareAndBind, SimpleQueueType } from "../internal/pubsub/consume.js";
async function main() {
  console.log("Starting Peril server...");
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  const ch = await conn.createConfirmChannel();
  console.log("Connected to RabbitMQ");
  await declareAndBind(conn, ExchangePerilTopic, "game_logs", "game_logs.*", SimpleQueueType.Durable)
  while (true) {
    const input = await getInput();
    if (input.length === 0) {
      continue;
    }
    if (input[0] === "pause") {
      console.log("sending pause message")
      await publishJSON(ch, ExchangePerilDirect, PauseKey, { isPaused: true })
    }
    if (input[0] === "resume") {
      console.log("Sending resume message")
      await publishJSON(ch, ExchangePerilDirect, PauseKey, { isPaused: false })
    }
    if (input[0] === "quit") {
      console.log("Exiting")
      break;
    }
    else {
      console.log(input[0])
    }
  }

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
