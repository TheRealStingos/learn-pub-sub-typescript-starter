import amqp from "amqplib";
import { clientWelcome, commandStatus, getInput, printClientHelp } from "../internal/gamelogic/gamelogic.js";
import { declareAndBind, SimpleQueueType } from "../internal/pubsub/consume.js";
import { ExchangePerilDirect } from "../internal/routing/routing.js";
import { PauseKey } from "../internal/routing/routing.js";
import { GameState } from "../internal/gamelogic/gamestate.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { commandMove } from "../internal/gamelogic/move.js";
import { subscribeJSON } from "../internal/pubsub/subscribe.js";
import { handlerPause } from "./handlers.js";

async function main() {
  console.log("Starting Peril client...");
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  const username = await clientWelcome();
  await declareAndBind(conn, ExchangePerilDirect, `pause.${username}`, PauseKey, SimpleQueueType.Transient);
  const gameState = new GameState(username)
  subscribeJSON(conn, ExchangePerilDirect, `pause.${username}`, PauseKey, SimpleQueueType.Transient, handlerPause(gameState))

  while (true) {
    const input = await getInput()
    if (input.length === 0) {
      continue
    }
    if (input[0] === "spawn") {
      commandSpawn(gameState, input)
      continue
    }
    if (input[0] === "move") {
      try {
        commandMove(gameState, input);
      } catch (err) {
        console.log("error")
      }
      continue
    }
    if (input[0] === "status") {
      commandStatus(gameState);
      continue
    }
    if (input[0] === "help") {
      printClientHelp();
      continue
    }
    if (input[0] === "spam") {
      console.log("Spamming is not allowed")
      continue
    }
    else {
      console.log("unknown command")
      continue;
    }

  }
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
