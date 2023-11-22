import { Injectable, OnModuleInit } from '@nestjs/common';

import { AdminService, ConsumerService } from './kafka/service';

@Injectable()
export class TestConsumer implements OnModuleInit {
    private readonly groupId = 'test-consumer';
    private readonly topics = ['test', 'test2', 'test3'];

    constructor(private readonly consumerService: ConsumerService, private readonly adminService: AdminService) {}

    async onModuleInit() {
        await this.adminService.printInfo({
            printTopics: false,
            printGroups: false,
            printInfo: false,
            printTopicsMetadata: false,
        });

        await this.consumerService.consume({
            topics: {
                topics: this.topics,

                // When fromBeginning is true, the group will use the earliest offset.
                // If set to false, it will use the latest offset. The default is false.
                fromBeginning: false,
            },
            consumerConfig: {
                // Configures the consumer isolation level. If false (default),
                // the consumer will not return any transactional messages which were not committed.
                readUncommitted: false,

                groupId: this.groupId,
                allowAutoTopicCreation: true,

                // Max number of requests that may be in progress at any time. If falsey then no limit.
                maxInFlightRequests: 1,

                // The maximum amount of time in milliseconds the server will block
                // before answering the fetch request if there isn’t sufficient data
                // to immediately satisfy the requirement given by minBytes
                maxWaitTimeInMs: 100,
            },
            runConfig: {
                autoCommit: false, //autoCommit: false: Disable automatic offset commits.
                eachMessage: async ({ topic, partition, message }) => {
                    const messageOffset = message.offset;
                    const messageTimestamp = message.timestamp;
                    const messageKey = message.key?.toString();
                    const messageValue = JSON.parse(message.value.toString());
                    const messageHeaders: Record<string, string> = {};
                    Object.keys(message.headers).forEach(
                        (headerKey) => (messageHeaders[headerKey] = message.headers[headerKey].toString()),
                    );

                    console.log('\n' + '='.repeat(50));
                    console.log('topic :>> ', topic);
                    console.log('partition :>>', partition);
                    console.log('messageOffset :>>', messageOffset);
                    console.log('messageSendDate :>> ', new Date(+messageTimestamp));
                    console.log('messageHeaders :>>', messageHeaders);
                    console.log('messageKey :>>', messageKey);
                    console.log('messageValue :>>', messageValue);
                    console.log('='.repeat(50) + '\n');

                    await this.processMessage({
                        topic,
                        correlationId: messageHeaders.correlationId,
                        version: messageHeaders.version,
                        key: messageKey,
                        message: messageValue,
                    });

                    await this.consumerService.commitOffsets({
                        groupId: this.groupId,
                        topicPartitions: [
                            {
                                topic,
                                partition,
                                // Store a message's offset + 1 in the store together with the results of processing.
                                // 1 is added to prevent that same message from being consumed again.
                                offset: messageOffset + 1,
                            },
                        ],
                    });
                },
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
