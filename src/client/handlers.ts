import type { ArmyMove } from "../internal/gamelogic/gamedata.js";
import type { GameState, PlayingState } from "../internal/gamelogic/gamestate.js";
import { handleMove, MoveOutcome } from "../internal/gamelogic/move.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import { AckType } from "../internal/pubsub/subscribe.js";

export function handlerPause(gs: GameState): (ps: PlayingState) => void {
    return (ps: PlayingState) => {
        handlePause(gs, ps);
        process.stdout.write("> ")
        AckType.Ack
    }
}

export function handlerMove(gs: GameState): (move: ArmyMove) => Promise<AckType> {
    return async (move: ArmyMove) => {
        const outcome = handleMove(gs, move);
        process.stdout.write("> ")
        if (outcome === MoveOutcome.Safe || outcome === MoveOutcome.MakeWar) {
            console.log("Ack: move outcome was", MoveOutcome[outcome])
            return AckType.Ack
        }
        else {
            console.log("NackDiscard: move outcome was", MoveOutcome[outcome])
            return AckType.NackDiscard
        }
    }
}