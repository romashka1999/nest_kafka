import { Logger } from '@nestjs/common';
import {
    Consumer,
    ConsumerConfig,
    ConsumerRunConfig,
    ConsumerSubscribeTopics,
    Kafka,
    TopicPartitionOffsetAndMetadata,
} from 'kafkajs';

import { sleep } from '../../../utils/sleep';

export class KafkajsConsumer {
    private readonly kafka: Kafka;
    private readonly consumer: Consumer;
    private readonly logger: Logger;

    constructor(
        private readonly consumerSubscribeTopics: ConsumerSubscribeTopics,
        consumerConfig: ConsumerConfig,
        broker: string,
        clientId: string,
    ) {
        this.kafka = new Kafka({ brokers: [broker], clientId });
        this.consumer = this.kafka.consumer(consumerConfig);
        this.logger = new Logger(`${consumerSubscribeTopics.topics.join(',')}---${consumerConfig.groupId}`);
    }

    async consume(runConfig: ConsumerRunConfig) {
        this.logger.log('Started consuming');
        await this.consumer.subscribe(this.consumerSubscribeTopics);
        await this.consumer.run(runConfig);
    }

    async commitOffsets(topicPartitions: TopicPartitionOffsetAndMetadata[]) {
        this.logger.debug('Commit offsets to ' + JSON.stringify(topicPartitions));
        await this.consumer.commitOffsets(topicPartitions);
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
