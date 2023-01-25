import { Message, RecordMetadata } from 'kafkajs';

export interface IProducer {
    connect: () => Promise<void>;
    disconnect: () => Promise<void>;
    produce: (params: { messages: Message[]; acks?: number; timeout?: number }) => Promise<RecordMetadata[]>;
}
