import { Injectable, OnModuleInit } from '@nestjs/common';

import { AdminService, ConsumerService } from './kafka/service';

@Injectable()
export class TestConsumer implements OnModuleInit {
    constructor(private readonly consumerService: ConsumerService, private readonly adminService: AdminService) {}

    async onModuleInit() {
        await this.adminService.printInfo({ printTopics: true, printGroups: true, printInfo: true });

        await this.consumerService.consume({
            topic: { topic: 'test' },
            config: { groupId: 'test-consumer' },
            onMessage: async (message) => {
                console.log({
                    value: message.value.toString(),
                });
                // throw new Error('Test error!');
            },
        });
    }
}
