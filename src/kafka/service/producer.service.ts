import { Injectable, OnApplicationShutdown } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Message } from 'kafkajs';

import { KafkajsProducer } from './kafkajs';
import { IProducer } from '../contract';

@Injectable()
export class ProducerService implements OnApplicationShutdown {
    private readonly producers = new Map<string, IProducer>();

    constructor(private readonly configService: ConfigService) {}

    async produce(topic: string, messages: Message | Message[], options?: { acks?: number; timeout?: number }) {
        const producer = await this.getProducer(topic);

        let msgs: Message[];

        if (Array.isArray(messages)) {
            msgs = messages;
        } else {
            msgs = [messages];
        }

        await producer.produce({ messages: msgs, ...options });
    }

    private async getProducer(topic: string) {
        let kafkajsProducer = this.producers.get(topic);
        if (!kafkajsProducer) {
            kafkajsProducer = new KafkajsProducer(
                topic,
                this.configService.get('KAFKA_BROKER'),
                this.configService.get('APP_NAME'),
            );
            await kafkajsProducer.connect();
            this.producers.set(topic, kafkajsProducer);
        }
        return kafkajsProducer;
    }

    async onApplicationShutdown() {
        for (const producer of this.producers.values()) {
            await producer.disconnect();
        }
    }
}
