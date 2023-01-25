import { Logger } from '@nestjs/common';
import { Kafka, Message, Partitioners, Producer } from 'kafkajs';

import { IProducer } from '../../contract';
import { sleep } from '../../../utils/sleep';

export class KafkajsProducer implements IProducer {
    private readonly kafka: Kafka;
    private readonly producer: Producer;
    private readonly logger: Logger;

    constructor(private readonly topic: string, broker: string, clientId: string) {
        this.kafka = new Kafka({
            brokers: [broker],
            clientId,
        });
        this.producer = this.kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
        this.logger = new Logger(KafkajsProducer.name);
    }

    produce({ messages, acks, timeout }: { messages: Message[]; acks?: number; timeout?: number }) {
        this.logger.debug('Produced message ' + JSON.stringify(messages));
        return this.producer.send({ topic: this.topic, messages, acks, timeout });
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
