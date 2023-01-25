import { Injectable } from '@nestjs/common';
import { v4 as uuidV4 } from 'uuid';

import { ProducerService } from './kafka/service/producer.service';

@Injectable()
export class AppService {
    constructor(private readonly producerService: ProducerService) {}

    async getHello() {
        await this.producerService.produce('test', {
            key: 'esarisKey',
            value: JSON.stringify({ txt: 'Hello World' }),
            headers: {
                correlationId: uuidV4(),
                version: '1',
            },
        });
        return 'Hello World!';
    }
}
