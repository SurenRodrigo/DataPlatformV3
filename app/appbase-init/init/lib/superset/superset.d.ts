import { AxiosInstance } from 'axios';
export declare function wrapApiError(cause: unknown, msg: string): Error;
export interface SupersetConfig {
    readonly url: string;
    readonly username: string;
    readonly password: string;
}
interface DatabaseConfig {
    readonly database_name: string;
    readonly sqlalchemy_uri: string;
    readonly engine: string;
    readonly configuration_method?: string;
    readonly expose_in_sqllab?: boolean;
    readonly allow_run_async?: boolean;
    readonly allow_ctas?: boolean;
    readonly allow_cvas?: boolean;
    readonly allow_dml?: boolean;
    readonly allow_file_upload?: boolean;
}
export declare class Superset {
    private readonly api;
    private readonly refreshToken;
    constructor(api: AxiosInstance, refreshToken: string);
    static fromConfig(cfg: SupersetConfig): Promise<Superset>;
    private static authenticate;
    refreshAccessToken(): Promise<void>;
    getDatabases(): Promise<any[]>;
    getDatabase(name: string): Promise<any | undefined>;
    createDatabase(config: DatabaseConfig): Promise<any>;
    updateDatabase(id: number, updates: Partial<DatabaseConfig>): Promise<any>;
    testDatabaseConnection(config: DatabaseConfig): Promise<boolean>;
    syncDatabase(databaseId: number): Promise<void>;
    syncDataset(datasetId: number): Promise<void>;
    getDatasetByName(databaseName: string, tableName?: string): Promise<any | undefined>;
    getTables(databaseId: number): Promise<any[]>;
    getDashboards(): Promise<any[]>;
    getDashboard(id: number): Promise<any>;
    createDashboard(dashboard: any): Promise<any>;
    updateDashboard(id: number, dashboard: any): Promise<any>;
    getCharts(): Promise<any[]>;
    getChart(id: number): Promise<any>;
    createChart(chart: any): Promise<any>;
    getDatasets(): Promise<any[]>;
    createDataset(dataset: any): Promise<any>;
    importDashboard(dashboardData: any): Promise<any>;
    exportDashboard(id: number): Promise<any>;
    healthCheck(): Promise<boolean>;
}
export {};
