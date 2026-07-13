import amqp from "amqplib";
import { type Channel } from "amqplib";
import { AckType } from "./subscribe.js";
import { decode } from "@msgpack/msgpack";

export enum SimpleQueueType {
    Durable,
    Transient,
}

export async function declareAndBind(
    conn: amqp.ChannelModel,
    exchange: string,
    queueName: string,
    key: string,
    queueType: SimpleQueueType,
): Promise<[Channel, amqp.Replies.AssertQueue]> {
    const ch = await conn.createChannel();
    const queue = await ch.assertQueue(queueName, {
        durable: queueType === SimpleQueueType.Durable,
        autoDelete: queueType === SimpleQueueType.Transient,
        exclusive: queueType === SimpleQueueType.Transient,
        arguments: { "x-dead-letter-exchange": "peril_dlx" }
    });
    await ch.assertExchange(exchange, "topic", { durable: true });
    await ch.bindQueue(queueName, exchange, key)
    return [ch, queue]
}

export async function subscribeMsgPack<T>(
    conn: amqp.ChannelModel,
    exchange: string,
    queueName: string,
    key: string,
    queueType: SimpleQueueType,
    handler: (data: T) => Promise<AckType> | AckType,
): Promise<void> {
    const [ch, assertQueue] = await declareAndBind(conn, exchange, queueName, key, queueType);
    await ch.prefetch(10);
    ch.consume(assertQueue.queue, async (message: amqp.ConsumeMessage | null) => {
        if (!message) {
            return
        }
        const buffer = message.content
        const decoded = decode(buffer)
        const ackType = await handler(decoded);
        if (ackType === AckType.Ack) {
            ch.ack(message);
        }
        if (ackType === AckType.NackRequeue) {
            ch.nack(message, false, true);
        }
        if (ackType === AckType.NackDiscard) {
            ch.nack(message, false, false);
        }
    })
}