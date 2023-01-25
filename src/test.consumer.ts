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
                    console.log({
                        value: message.value.toString(),
                    });

                    await this.consumerService.commitOffsets({
                        consumerId,
                        topicPartitions: [{ topic, partition, offset: message.offset }],
                    });
                },
                autoCommit: false,
            },
        });
    }
}
