import { Logger } from '@nestjs/common';
import { Consumer, ConsumerConfig, ConsumerSubscribeTopic, Kafka, KafkaMessage } from 'kafkajs';
import * as retry from 'async-retry';

import { sleep } from '../../utils/sleep';
import { IConsumer } from '../contract';
import { DatabaseService } from '../../database/database.service';

export class KafkajsConsumer implements IConsumer {
    private readonly kafka: Kafka;
    private readonly consumer: Consumer;
    private readonly logger: Logger;

    constructor(
        private readonly consumerSubscribeTopic: ConsumerSubscribeTopic,
        private readonly databaseService: DatabaseService,
        config: ConsumerConfig,
        broker: string,
    ) {
        this.kafka = new Kafka({ brokers: [broker] });
        this.consumer = this.kafka.consumer(config);
        this.logger = new Logger(`${consumerSubscribeTopic.topic}-${config.groupId}`);
    }

    async consume(onMessage: (message: KafkaMessage) => Promise<void>) {
        await this.consumer.subscribe(this.consumerSubscribeTopic);
        await this.consumer.run({
            eachMessage: async ({ message, partition, topic }) => {
                this.logger.debug(`Processing message topic ${topic} partition: ${partition}`);
                try {
                    await retry(async () => onMessage(message), {
                        retries: 3,
                        onRetry: (error, attempt) =>
                            this.logger.error(`Error consuming message, executing retry ${attempt}/3...`, error),
                    });
                } catch (err) {
                    this.logger.error('Error consuming message. Adding to dead letter queue...', err);
                    await this.addMessageToDlq(message);
                }
            },
        });
    }

    private async addMessageToDlq(message: KafkaMessage) {
        await this.databaseService
            .getDbHandle()
            .collection('dlq')
            .insertOne({ value: message.value, topic: this.consumerSubscribeTopic.topic });
    }

    async connect() {
        try {
            await this.consumer.connect();
        } catch (err) {
            this.logger.error('Failed to connect to Kafka.', err);
            await sleep(5000);
            await this.connect();
        }
    }

    async disconnect() {
        await this.consumer.disconnect();
    }
}
