import amqp from "amqplib"
import { declareAndBind, SimpleQueueType } from "./consume.js"

export async function subscribeJSON<T>(
    conn: amqp.ChannelModel,
    exchange: string,
    queueName: string,
    key: string,
    queueType: SimpleQueueType,
    handler: (data: T) => void,
): Promise<void> {
    const [ch, assertQueue] = await declareAndBind(conn, exchange, queueName, key, queueType);
    ch.consume(assertQueue.queue, (message: amqp.ConsumeMessage | null) => {
        if (!message) {
            return
        }
        const buffer = message.content
        const stringified = buffer.toString()
        const parsed = JSON.parse(stringified)

        handler(parsed);
        ch.ack(message);
    })
}
