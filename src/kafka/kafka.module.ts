import { Module } from '@nestjs/common';

import { ProducerService, ConsumerService, AdminService } from './service';
import { DatabaseModule } from '../database/database.module';

@Module({
    imports: [DatabaseModule],
    providers: [ProducerService, ConsumerService, AdminService],
    exports: [ProducerService, ConsumerService, AdminService],
})
export class KafkaModule {}
