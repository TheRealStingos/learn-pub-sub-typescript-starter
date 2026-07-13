import type { ConfirmChannel } from "amqplib";
import type { ArmyMove } from "../internal/gamelogic/gamedata.js";
import type { GameState, PlayingState } from "../internal/gamelogic/gamestate.js";
import { handleMove, MoveOutcome } from "../internal/gamelogic/move.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import { AckType } from "../internal/pubsub/subscribe.js";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilTopic } from "../internal/routing/routing.js";
import { type RecognitionOfWar } from "../internal/gamelogic/gamedata.js";
import { WarRecognitionsPrefix } from "../internal/routing/routing.js";
import { handleWar, WarOutcome, type WarResolution } from "../internal/gamelogic/war.js";
import { publishGameLog } from "./index.js";

export function handlerPause(gs: GameState): (ps: PlayingState) => Promise<AckType> {
    return async (ps: PlayingState) => {
        handlePause(gs, ps);
        process.stdout.write("> ")
        return AckType.Ack
    }
}

export function handlerMove(gs: GameState, ch: ConfirmChannel): (move: ArmyMove) => Promise<AckType> {
    return async (move: ArmyMove) => {
        const outcome = handleMove(gs, move);
        process.stdout.write("> ")
        if (outcome === MoveOutcome.Safe) {
            console.log("Ack: move outcome was", MoveOutcome[outcome])
            return AckType.Ack
        }
        if (outcome === MoveOutcome.MakeWar) {
            console.log("Nack Req: move outcome was", MoveOutcome[outcome])
            const rw: RecognitionOfWar = {
                attacker: move.player,
                defender: gs.getPlayerSnap(),
            };
            try {
                await publishJSON(ch, ExchangePerilTopic, `${WarRecognitionsPrefix}.${gs.getPlayerSnap().username}`, rw)
            } catch {
                return AckType.NackRequeue
            }
            return AckType.Ack
        }
        else {
            console.log("NackDiscard: move outcome was", MoveOutcome[outcome])
            return AckType.NackDiscard
        }
    }
}

export function handlerWar(gs: GameState, ch: ConfirmChannel): (rw: RecognitionOfWar) => Promise<AckType> {
    return async (rw: RecognitionOfWar) => {
        console.log("war handler reached")
        const outcome = handleWar(gs, rw)
        process.stdout.write("> ")
        if (outcome.result === WarOutcome.NotInvolved) {
            return AckType.NackRequeue
        }

        if (outcome.result === WarOutcome.NoUnits) {
            return AckType.NackDiscard
        }

        if (outcome.result === WarOutcome.OpponentWon) {
            try {
                await publishGameLog(ch, gs.getUsername(), `${outcome.winner} won a war against ${outcome.loser}`)
            } catch {
                return AckType.NackRequeue
            }
            return AckType.Ack
        }

        if (outcome.result === WarOutcome.YouWon) {
            try {
                await publishGameLog(ch, gs.getUsername(), `${outcome.winner} won a war against ${outcome.loser}`)
            } catch {
                return AckType.NackRequeue
            }
            return AckType.Ack
        }

        if (outcome.result === WarOutcome.Draw) {
            try {
                await publishGameLog(ch, gs.getUsername(), `A war between ${outcome.attacker} and ${outcome.defender} resulted in a draw`)
            } catch {
                return AckType.NackRequeue
            }
            return AckType.Ack
        }

        else {
            console.log("error")
            return AckType.NackDiscard
        }
    }
}