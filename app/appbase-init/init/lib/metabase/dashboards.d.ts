import pino from 'pino';
interface DatabaseMetadata {
    readonly tableIds: Record<string, number>;
    readonly fieldIds: Record<string, Record<string, number>>;
    readonly tableNames: Record<number, string>;
    readonly fieldNames: Record<number, string>;
}
interface MetabaseConfig {
    readonly url: string;
    readonly username: string;
    readonly password: string;
}
interface DashboardsConfig {
    readonly metabase: MetabaseConfig;
    readonly databaseName: string;
    readonly logger: pino.Logger;
}
export declare class Dashboards {
    private readonly metabase;
    private readonly databaseId;
    private readonly logger;
    private constructor();
    static fromConfig(cfg: DashboardsConfig): Promise<Dashboards>;
    getDatabaseMetadata(): Promise<DatabaseMetadata>;
    /** Exports a dashboard to a template string */
    export(dashboardOrId: number | any): Promise<string>;
    /** Recursive function to replace ids with named references */
    private templatize;
    /**
     * Sync database metadata based on tables and fields referenced
     * in dashboard templates
     */
    private syncTemplateMetadata;
    /** Get fields referenced in template by name */
    private getFields;
    /** Returns a map from card name to its parent card name */
    private getCardDependencies;
    private renderTemplate;
    importNew(): Promise<void>;
    importOne(name: string): Promise<void>;
    private import;
    private createCards;
    getOrCreateCollection(path: string): Promise<number>;
    private static parentCardId;
    private static tableName;
    private static isJSON;
    private static equalCards;
}
export {};
