import { Logger } from '@nestjs/common';
import { Admin, ITopicConfig, Kafka } from 'kafkajs';

import { IAdmin } from '../../contract';
import { sleep } from '../../../utils/sleep';

export class KafkajsAdmin implements IAdmin {
    private readonly kafka: Kafka;
    private readonly admin: Admin;
    private readonly logger: Logger;

    constructor(broker: string, clientId: string) {
        this.kafka = new Kafka({
            clientId,
            brokers: [broker],
        });
        this.admin = this.kafka.admin();
        this.logger = new Logger('admin');
    }

    async getAllGroups() {
        const { groups } = await this.admin.listGroups();
        return groups;
    }

    getAllTopics() {
        return this.admin.listTopics();
    }

    async createTopics(options: {
        validateOnly?: boolean;
        waitForLeaders?: boolean;
        timeout?: number;
        topics: ITopicConfig[];
    }) {
        const success = await this.admin.createTopics({ ...options });

        return {
            success,
        };
    }

    async deleteTopics(options: { topics: string[]; timeout?: number }) {
        await this.admin.deleteTopics({ ...options });

        this.admin.describeCluster();
    }

    async getInfo(inputGroupIds?: string[]) {
        const groupIds = inputGroupIds || (await this.getAllGroups()).map((g) => g.groupId);

        const [cluster, groups]: [
            Awaited<ReturnType<typeof this.admin.describeCluster>>,
            Awaited<ReturnType<typeof this.admin.describeGroups>>,
        ] = await Promise.all([this.admin.describeCluster(), this.admin.describeGroups(groupIds)]);

        return {
            cluster,
            groups,
        };
    }

    async connect() {
        try {
            await this.admin.connect();
        } catch (err) {
            this.logger.error('Failed to connect to Kafka.', err);
            await sleep(5000);
            await this.connect();
        }
    }

    async disconnect() {
        await this.admin.disconnect();
    }
}
