import { Injectable, OnApplicationShutdown } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { ConsumerConfig, ConsumerRunConfig, ConsumerSubscribeTopics } from 'kafkajs';

import { KafkajsConsumer } from './kafkajs';
import { IConsumer } from '../contract';

interface KafkajsConsumerOptions {
    topics: ConsumerSubscribeTopics;
    consumerConfig: ConsumerConfig;
    runConfig: ConsumerRunConfig;
}

@Injectable()
export class ConsumerService implements OnApplicationShutdown {
    private readonly consumers: IConsumer[] = [];

    constructor(private readonly configService: ConfigService) {}

    async consume({ topics, consumerConfig, runConfig }: KafkajsConsumerOptions) {
        const consumer = new KafkajsConsumer(
            topics,
            consumerConfig,
            this.configService.get('KAFKA_BROKER'),
            this.configService.get('APP_NAME'),
        );
        await consumer.connect();
        await consumer.consume(runConfig);
        this.consumers.push(consumer);
    }

    async onApplicationShutdown() {
        for (const consumer of this.consumers) {
            await consumer.disconnect();
        }
    }
}
