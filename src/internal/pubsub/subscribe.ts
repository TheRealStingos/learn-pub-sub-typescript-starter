import amqp from "amqplib"
import { declareAndBind, SimpleQueueType } from "./consume.js"

export enum AckType {
    Ack,
    NackRequeue,
    NackDiscard,
}

export async function subscribeJSON<T>(
    conn: amqp.ChannelModel,
    exchange: string,
    queueName: string,
    key: string,
    queueType: SimpleQueueType,
    handler: (data: T) => Promise<AckType> | AckType,
): Promise<void> {
    const [ch, assertQueue] = await declareAndBind(conn, exchange, queueName, key, queueType);
    ch.consume(assertQueue.queue, async (message: amqp.ConsumeMessage | null) => {
        if (!message) {
            return
        }
        const buffer = message.content
        const stringified = buffer.toString()
        const parsed = JSON.parse(stringified)

        const ackType = await handler(parsed);
        if (ackType === AckType.Ack) {
            ch.ack(message);
            console.log("Ack")
        }
        if (ackType === AckType.NackRequeue) {
            ch.nack(message, false, true);
            console.log("Nack")
        }
        if (ackType === AckType.NackDiscard) {
            ch.nack(message, false, false);
            console.log("Nack Dis")
        }
    })
}
