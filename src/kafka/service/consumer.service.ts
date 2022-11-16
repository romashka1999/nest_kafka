import { Injectable, OnApplicationShutdown } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { ConsumerConfig, ConsumerSubscribeTopic, KafkaMessage } from 'kafkajs';

import { KafkajsConsumer } from './kafkajs';
import { IConsumer } from '../contract';
import { DatabaseService } from '../../database/database.service';

interface KafkajsConsumerOptions {
    topic: ConsumerSubscribeTopic;
    config: ConsumerConfig;
    onMessage: (message: KafkaMessage) => Promise<void>;
}

@Injectable()
export class ConsumerService implements OnApplicationShutdown {
    private readonly consumers: IConsumer[] = [];

    constructor(private readonly configService: ConfigService, private readonly databaserService: DatabaseService) {}

    async consume({ topic, config, onMessage }: KafkajsConsumerOptions) {
        const consumer = new KafkajsConsumer(
            topic,
            this.databaserService,
            config,
            this.configService.get('KAFKA_BROKER'),
            this.configService.get('APP_NAME'),
        );
        await consumer.connect();
        await consumer.consume(onMessage);
        this.consumers.push(consumer);
    }

    async onApplicationShutdown() {
        for (const consumer of this.consumers) {
            await consumer.disconnect();
        }
    }
}
