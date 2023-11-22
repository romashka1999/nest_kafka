import { Injectable, OnApplicationShutdown } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Message } from 'kafkajs';

import { KafkajsProducer } from './kafkajs';

@Injectable()
export class ProducerService implements OnApplicationShutdown {
    private producer: KafkajsProducer;

    constructor(private readonly configService: ConfigService) {}

    async produce(topic: string, messages: Message | Message[], options?: { acks?: number; timeout?: number }) {
        const producer = await this.getProducer();

        let msgs: Message[];

        if (Array.isArray(messages)) {
            msgs = messages;
        } else {
            msgs = [messages];
        }

        return producer.produce({ topic, messages: msgs, ...options });
    }

    private async getProducer() {
        let kafkajsProducer = this.producer;
        if (!kafkajsProducer) {
            kafkajsProducer = new KafkajsProducer(
                this.configService.get('KAFKA_BROKER'),
                this.configService.get('APP_NAME'),
            );
            await kafkajsProducer.connect();
            this.producer = kafkajsProducer;
        }
        return kafkajsProducer;
    }

    async onApplicationShutdown() {
        await this.producer.disconnect();
    }
}
