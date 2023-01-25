import { Injectable, OnModuleInit } from '@nestjs/common';

import { AdminService, ConsumerService } from './kafka/service';

@Injectable()
export class TestConsumer implements OnModuleInit {
    private readonly groupId = 'test-consumer';
    private readonly topics = ['test1', 'test2', 'test3'];

    constructor(private readonly consumerService: ConsumerService, private readonly adminService: AdminService) {}

    async onModuleInit() {
        await this.adminService.printInfo({
            printTopics: false,
            printGroups: false,
            printInfo: false,
            printTopicsMetadata: false,
        });

        await this.consumerService.consume({
            topics: { topics: this.topics, fromBeginning: true },
            consumerConfig: { groupId: this.groupId, allowAutoTopicCreation: true },
            runConfig: {
                eachMessage: async ({ topic, partition, message }) => {
                    const messageHeaders = message.headers;
                    const messageOffset = message.offset;
                    const messageKey = message.key?.toString();
                    const messageValue = JSON.parse(message.value.toString());

                    console.log('messageHeaders :>>', messageHeaders);
                    console.log('messageOffset :>>', messageOffset);
                    console.log('messageKey :>>', messageKey);
                    console.log('messageHeaders :>>', messageValue);

                    await this.processMessage({
                        topic,
                        correlationId: messageHeaders.correlationId.toString(),
                        version: messageHeaders.version.toString(),
                        key: messageKey,
                        message: messageValue,
                    });

                    await this.consumerService.commitOffsets({
                        groupId: this.groupId,
                        topicPartitions: [{ topic, partition, offset: messageOffset }],
                    });
                },
                autoCommit: false,
            },
        });
    }

    private async processMessage(params: {
        topic: string;
        correlationId: string;
        version: string;
        key: string;
        message: object;
    }) {
        const { topic, correlationId, version } = params;

        // correlationId event set pending

        if (topic === 'test1') {
            if (version === '1') {
                //ragaca logika
            }
        } else if (topic === 'test2') {
            if (version === '1') {
                //ragaca logika
            }
        } else if (topic === 'asdasd') {
            if (version === '1') {
                //ragaca logika
            }
        }
    }
}
