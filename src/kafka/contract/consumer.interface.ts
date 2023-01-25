import { ConsumerRunConfig } from 'kafkajs';

export interface IConsumer {
    connect: () => Promise<void>;
    disconnect: () => Promise<void>;
    consume: (runConfig: ConsumerRunConfig) => Promise<void>;
}
