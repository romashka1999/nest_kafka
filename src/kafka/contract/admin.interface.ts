import { GroupDescriptions, ITopicConfig } from 'kafkajs';

export interface IAdmin {
    connect: () => Promise<void>;
    disconnect: () => Promise<void>;
    getAllTopics: () => Promise<string[]>;
    getAllGroups: () => Promise<{ groupId: string; protocolType: string }[]>;
    createTopics: (options: {
        validateOnly?: boolean;
        waitForLeaders?: boolean;
        timeout?: number;
        topics: ITopicConfig[];
    }) => Promise<{ success: boolean }>;
    deleteTopics: (options: { topics: string[]; timeout?: number }) => Promise<void>;
    getInfo(groupIds?: string[]): Promise<{
        cluster: {
            brokers: Array<{ nodeId: number; host: string; port: number }>;
            controller: number | null;
            clusterId: string;
        };
        groups: GroupDescriptions;
    }>;
}
