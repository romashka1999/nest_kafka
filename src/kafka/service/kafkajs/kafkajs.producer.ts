import { Logger } from '@nestjs/common';
import { Kafka, Partitioners, Producer, ProducerRecord } from 'kafkajs';

import { sleep } from '../../../utils/sleep';

export class KafkajsProducer {
    private readonly kafka: Kafka;
    private readonly producer: Producer;
    private readonly logger: Logger;

    constructor(broker: string, clientId: string) {
        this.kafka = new Kafka({
            brokers: [broker],
            clientId,
        });
        this.producer = this.kafka.producer({
            createPartitioner: Partitioners.DefaultPartitioner,
            idempotent: true, // idempotent: true: Enable idempotent producer. This ensures that messages sent to the broker are either successfully written or result in an error, avoiding duplicate writes.
            maxInFlightRequests: 1, // maxInFlightRequests: Set the maximum number of unacknowledged requests the producer will allow.
        });
        this.logger = new Logger(KafkajsProducer.name);
    }

    produce(producerRecord: ProducerRecord) {
        this.logger.debug('Produced message ' + JSON.stringify(producerRecord.messages));
        return this.producer.send(producerRecord);
    }

    async transaction(producerRecord: ProducerRecord) {
        const transaction = await this.producer.transaction();

        try {
            await transaction.send(producerRecord);

            await transaction.commit();
        } catch (e) {
            this.logger.error('Producer transaction failed ' + e);
            await transaction.abort();
        }
    }

    async connect() {
        try {
            await this.producer.connect();
        } catch (err) {
            this.logger.error('Failed to connect to Kafka.', err);
            await sleep(5000);
            await this.connect();
        }
    }

    async disconnect() {
        await this.producer.disconnect();
    }
}
