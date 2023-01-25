import { ConsumerRunConfig, TopicPartitionOffsetAndMetadata } from 'kafkajs';

export interface IConsumer {
    connect: () => Promise<void>;
    disconnect: () => Promise<void>;
    consume: (runConfig: ConsumerRunConfig) => Promise<void>;
    commitOffsets: (topicPartitions: Array<TopicPartitionOffsetAndMetadata>) => Promise<void>;
}
