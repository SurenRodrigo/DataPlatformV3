import { AxiosInstance } from 'axios';
import pino from 'pino';
import { ForeignKey, Source } from './types';
export declare class HasuraInit {
    private readonly api;
    private readonly logger;
    private readonly resourcesDir;
    constructor(api: AxiosInstance, logger: pino.Logger, resourcesDir?: string);
    private listAllTables;
    private listAllForeignKeys;
    private getMetadata;
    private getDbSource;
    /**
     * Dynamically creates the 'default' source in Hasura if it does not exist.
     * Uses the Hasura Metadata API (pg_add_source) with the provided database URL.
     * This is idempotent and safe to call if the source is missing.
     */
    private createDefaultSource;
    private getQueryCollections;
    private getEndpoints;
    private trackTable;
    private createObjectRelationship;
    private createArrayRelationship;
    private createSelectPermission;
    private createInsertPermission;
    private createUpdatePermission;
    private createDeletePermission;
    private setupTablePermissions;
    initializePermissionSystem(): Promise<void>;
    private loadMetadata;
    static createSourceMetadata(tableNames: ReadonlyArray<string>, foreignKeys: ReadonlyArray<ForeignKey>, databaseUrl?: string): Source;
    private loadQueryCollectionFromResources;
    private addQueryToCollection;
    private addEndpoint;
    private updateQueryCollections;
    private updateEndpoints;
    createEndpoints(): Promise<void>;
    trackAllTablesAndRelationships(databaseUrl?: string): Promise<void>;
}
