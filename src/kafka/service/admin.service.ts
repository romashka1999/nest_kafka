import { Injectable, OnApplicationShutdown } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';

import { KafkajsAdmin } from './kafkajs';
import { IAdmin } from '../contract';
import { DatabaseService } from '../../database/database.service';

@Injectable()
export class AdminService implements OnApplicationShutdown {
    private readonly admins: IAdmin[] = [];

    constructor(private readonly configService: ConfigService, private readonly databaserService: DatabaseService) {}

    async printInfo(opt: { printTopics: boolean; printGroups: boolean; printInfo: boolean }) {
        const admin = new KafkajsAdmin(this.configService.get('KAFKA_BROKER'), this.configService.get('APP_NAME'));
        await admin.connect();

        if (opt.printTopics) {
            const allTopics = await admin.getAllTopics();
            console.log('allTopics :>> ', allTopics, '\n', '='.repeat(60));
        }

        if (opt.printGroups) {
            const allGroups = await admin.getAllGroups();
            console.log('allGroups :>> ', allGroups, '\n', '='.repeat(60));
        }

        if (opt.printInfo) {
            const info = await admin.getInfo();
            console.log('info :>> ', info, '\n', '='.repeat(60));
        }

        this.admins.push(admin);
    }

    async onApplicationShutdown() {
        for (const admin of this.admins) {
            await admin.disconnect();
        }
    }
}
