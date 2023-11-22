import { Injectable, OnApplicationShutdown } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { ConsumerConfig, ConsumerRunConfig, ConsumerSubscribeTopics, TopicPartitionOffsetAndMetadata } from 'kafkajs';

import { KafkajsConsumer } from './kafkajs';

interface KafkajsConsumerOptions {
    topics: ConsumerSubscribeTopics;
    consumerConfig: ConsumerConfig;
    runConfig: ConsumerRunConfig;
}

@Injectable()
export class ConsumerService implements OnApplicationShutdown {
    private readonly consumersHashMap: Map<string, KafkajsConsumer> = new Map();

    constructor(private readonly configService: ConfigService) {}

    async consume({ topics, consumerConfig, runConfig }: KafkajsConsumerOptions) {
        const groupId = consumerConfig.groupId;

        if (this.consumersHashMap.has(groupId)) {
            throw new Error(`Consumer with groupId: ${groupId} already exists`);
        }

        const kafkajsConsumer = new KafkajsConsumer(
            topics,
            consumerConfig,
            this.configService.get('KAFKA_BROKER'),
            this.configService.get('APP_NAME'),
        );

        await kafkajsConsumer.connect();
        await kafkajsConsumer.consume(runConfig);

        this.consumersHashMap.set(groupId, kafkajsConsumer);
    }

    async commitOffsets(params: { groupId: string; topicPartitions: Array<TopicPartitionOffsetAndMetadata> }) {
        const { groupId, topicPartitions } = params;

        const consumer = this.consumersHashMap.get(groupId);
        await consumer.commitOffsets(topicPartitions);
    }

    async onApplicationShutdown() {
        const consumers = Array.from(this.consumersHashMap.values());

        for (const consumer of consumers) {
            await consumer.disconnect();
        }
    }
}
