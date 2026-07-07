import { type ConfirmChannel } from "amqplib";

export function publishJSON<T>(
    ch: ConfirmChannel,
    exchange: string,
    routingKey: string,
    value: T,
): Promise<void> {
    const serialized = Buffer.from(JSON.stringify(value));
    return new Promise((resolve, reject) => {
        ch.publish(exchange, routingKey, serialized, { contentType: "application/json" }, (err, result) => {
            if (err) {
                reject(err)
            } else {
                resolve()
            }
        });
    })
}