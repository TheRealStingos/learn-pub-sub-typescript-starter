import amqp, { type ConfirmChannel } from "amqplib";
import { clientWelcome, commandStatus, getInput, printClientHelp } from "../internal/gamelogic/gamelogic.js";
import { declareAndBind, SimpleQueueType } from "../internal/pubsub/consume.js";
import { ExchangePerilDirect, ExchangePerilTopic, GameLogSlug } from "../internal/routing/routing.js";
import { PauseKey } from "../internal/routing/routing.js";
import { GameState } from "../internal/gamelogic/gamestate.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { commandMove, handleMove } from "../internal/gamelogic/move.js";
import { subscribeJSON } from "../internal/pubsub/subscribe.js";
import { handlerMove, handlerPause, handlerWar } from "./handlers.js";
import { publishJSON, publishMsgPack } from "../internal/pubsub/publish.js";
import { WarRecognitionsPrefix } from "../internal/routing/routing.js";
import { type GameLog } from "../internal/gamelogic/logs.js";

async function main() {
  console.log("Starting Peril client...");
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  const ch = await conn.createConfirmChannel();
  const username = await clientWelcome();
  await declareAndBind(conn, ExchangePerilDirect, `pause.${username}`, PauseKey, SimpleQueueType.Transient);
  await declareAndBind(conn, ExchangePerilTopic, `army_moves.${username}`, "army_moves.*", SimpleQueueType.Transient)
  const gameState = new GameState(username)
  subscribeJSON(conn, ExchangePerilDirect, `pause.${username}`, PauseKey, SimpleQueueType.Transient, handlerPause(gameState))
  subscribeJSON(conn, ExchangePerilTopic, `army_moves.${username}`, "army_moves.*", SimpleQueueType.Transient, handlerMove(gameState, ch))
  subscribeJSON(conn, ExchangePerilTopic, "war", `${WarRecognitionsPrefix}.*`, SimpleQueueType.Durable, handlerWar(gameState, ch))

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
        const move = commandMove(gameState, input);
        publishJSON(ch, ExchangePerilTopic, `army_moves.${username}`, move)
        console.log("move was published successfully")
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

export function publishGameLog(ch: ConfirmChannel, username: string, msg: string) {
  const log: GameLog = {
    currentTime: new Date,
    username: username,
    message: msg
  }

  publishMsgPack(ch, ExchangePerilTopic, `${GameLogSlug}.${username}`, log)
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
