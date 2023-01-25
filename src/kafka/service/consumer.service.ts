import { Injectable, OnApplicationShutdown } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { ConsumerConfig, ConsumerRunConfig, ConsumerSubscribeTopics, TopicPartitionOffsetAndMetadata } from 'kafkajs';
import { v4 as uuidV4 } from 'uuid';

import { KafkajsConsumer } from './kafkajs';
import { IConsumer } from '../contract';

interface KafkajsConsumerOptions {
    topics: ConsumerSubscribeTopics;
    consumerConfig: ConsumerConfig;
    runConfig: ConsumerRunConfig;
}

@Injectable()
export class ConsumerService implements OnApplicationShutdown {
    private readonly consumersHashMap: Map<string, IConsumer> = new Map();

    constructor(private readonly configService: ConfigService) {}

    async consume({ topics, consumerConfig, runConfig }: KafkajsConsumerOptions) {
        const kafkajsConsumer = new KafkajsConsumer(
            topics,
            consumerConfig,
            this.configService.get('KAFKA_BROKER'),
            this.configService.get('APP_NAME'),
        );

        await kafkajsConsumer.connect();
        await kafkajsConsumer.consume(runConfig);

        let id: string = uuidV4();
        while (this.consumersHashMap.has(id)) {
            id = uuidV4();
        }

        this.consumersHashMap.set(id, kafkajsConsumer);

        return {
            consumerId: id,
        };
    }

    async commitOffsets(params: { consumerId: string; topicPartitions: Array<TopicPartitionOffsetAndMetadata> }) {
        const { consumerId, topicPartitions } = params;

        const consumer = this.consumersHashMap.get(consumerId);
        await consumer.commitOffsets(topicPartitions);
    }

    async onApplicationShutdown() {
        const consumers = Array.from(this.consumersHashMap.values());

        for (const consumer of consumers) {
            await consumer.disconnect();
        }
    }
}
