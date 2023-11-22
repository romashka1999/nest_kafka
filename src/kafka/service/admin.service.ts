import { inspect } from 'util';
import { Injectable, OnApplicationShutdown } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';

import { KafkajsAdmin } from './kafkajs';

@Injectable()
export class AdminService implements OnApplicationShutdown {
    private readonly admins: KafkajsAdmin[] = [];

    constructor(private readonly configService: ConfigService) {}

    async printInfo(opt: {
        printTopics: boolean;
        printGroups: boolean;
        printInfo: boolean;
        printTopicsMetadata: boolean;
    }) {
        const admin = new KafkajsAdmin(this.configService.get('KAFKA_BROKER'), this.configService.get('APP_NAME'));
        await admin.connect();

        if (opt.printTopics) {
            const allTopics = await admin.getAllTopics();
            console.log('allTopics :>> ', allTopics, '\n', '='.repeat(60));
        }

        if (opt.printGroups) {
            const allGroups = await admin.getAllGroups();
            console.log('allGroups :>> ', inspect(allGroups, { depth: null, showHidden: true }), '\n', '='.repeat(60));
        }

        if (opt.printInfo) {
            const info = await admin.getInfo();
            console.log('info :>> ', inspect(info, { depth: null, showHidden: true }), '\n', '='.repeat(60));
        }

        if (opt.printTopicsMetadata) {
            const topicsMetadata = await admin.getTopicsMetadata({});
            console.log(
                'topicsMetadata :>> ',
                inspect(topicsMetadata, { depth: null, showHidden: true }),
                '\n',
                '='.repeat(60),
            );
        }

        this.admins.push(admin);
    }

    async getAdmin() {
        const admin = new KafkajsAdmin(this.configService.get('KAFKA_BROKER'), this.configService.get('APP_NAME'));
        await admin.connect();
        this.admins.push(admin);
        return admin;
    }

    async onApplicationShutdown() {
        for (const admin of this.admins) {
            await admin.disconnect();
        }
    }
}
