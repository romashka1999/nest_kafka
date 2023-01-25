import { Injectable, OnModuleInit } from '@nestjs/common';

import { AdminService, ConsumerService } from './kafka/service';

@Injectable()
export class TestConsumer implements OnModuleInit {
    constructor(private readonly consumerService: ConsumerService, private readonly adminService: AdminService) {}

    async onModuleInit() {
        await this.adminService.printInfo({
            printTopics: false,
            printGroups: false,
            printInfo: false,
            printTopicsMetadata: false,
        });

        const { consumerId } = await this.consumerService.consume({
            topics: { topics: ['test'], fromBeginning: true },
            consumerConfig: { groupId: 'test-consumer', allowAutoTopicCreation: true },
            runConfig: {
                eachMessage: async ({ topic, partition, message }) => {
                    const messageHeaders = message.headers;
                    const messageOffset = message.offset;
                    const messageKey = message.key?.toString();
                    const messageValue = JSON.parse(message.value?.toString());

                    console.log('messageHeaders :>>', messageHeaders);
                    console.log('messageOffset :>>', messageOffset);
                    console.log('messageKey :>>', messageKey);
                    console.log('messageHeaders :>>', messageValue);

                    await this.consumerService.commitOffsets({
                        consumerId,
                        topicPartitions: [{ topic, partition, offset: messageOffset }],
                    });
                },
                autoCommit: false,
            },
        });
    }
}
