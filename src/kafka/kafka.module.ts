import { Module } from '@nestjs/common';

import { ProducerService, ConsumerService, AdminService } from './service';

@Module({
    providers: [ProducerService, ConsumerService, AdminService],
    exports: [ProducerService, ConsumerService, AdminService],
})
export class KafkaModule {}
