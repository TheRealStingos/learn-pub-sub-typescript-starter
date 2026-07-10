import { type ConfirmChannel } from "amqplib";
import { encode } from "@msgpack/msgpack";

export function publishJSON<T>(
    ch: ConfirmChannel,
    exchange: string,
    routingKey: string,
    value: T,
): Promise<void> {
    const serialized = Buffer.from(JSON.stringify(value));
    return new Promise((resolve, reject) => {
        ch.publish(exchange, routingKey, serialized, { contentType: "application/json" }, (err, result) => {
            if (err !== null) {
                reject(err)
            } else {
                resolve()
            }
        });
    })
}

export function publishMsgPack<T>(
  ch: ConfirmChannel,
  exchange: string,
  routingKey: string,
  value: T,
): Promise<void> {
    const encoded = encode(value)
    const serialized = Buffer.from(encoded)
    return new Promise((resolve, reject) => {
        ch.publish(exchange, routingKey, serialized, { contentType: "application/x-msgpack"}, (err, result) => {
            if (err !== null) {
                reject(err)
            } else {
                resolve()
            }
        })
    })
}