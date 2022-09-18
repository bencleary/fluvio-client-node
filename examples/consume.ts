/* tslint:disable:no-console */
import Fluvio, { ConsumerConfigWrapper, Offset, Record } from '@fluvio/client'

const TOPIC_NAME = 'mqtt'
const PARTITION = 0

async function consume() {
    try {
        const fluvio = new Fluvio()

        console.log('connecting client to fluvio')

        // Connect to the fluvio cluster referenced in the cli profile.
        await fluvio.connect()

        // Create partition consumer
        const consumer = await fluvio.partitionConsumer(TOPIC_NAME, PARTITION)

        console.log('read from the beginning')

        let file_path =
            '/Users/bencleary/Development/device-filter/target/wasm32-unknown-unknown/debug/device_filter.wasm'
        await consumer.stream_with_config(
            Offset.FromBeginning(),
            file_path,
            async (record: Record) => {
                // handle record;
                console.log(
                    `Key=${record.keyString()}, Value=${record.valueString()}`
                )
            }
        )

    } catch (ex) {
        console.log('error', ex)
    }
}

consume()
