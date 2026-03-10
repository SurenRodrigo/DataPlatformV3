import { AxiosInstance } from 'axios';
export declare function wrapApiError(cause: unknown, msg: string): Error;
export interface AttributeMappings {
    readonly id: number;
    readonly group_id: number;
    readonly table_id: number;
    readonly card_id?: number;
    readonly attribute_mappings: Map<string, string | number>;
}
export interface MetabaseConfig {
    readonly url: string;
    readonly username: string;
    readonly password: string;
}
interface FieldParams {
    [param: string]: any;
}
export declare class Metabase {
    private readonly api;
    constructor(api: AxiosInstance);
    static fromConfig(cfg: MetabaseConfig): Promise<Metabase>;
    private static sessionToken;
    getDatabases(): Promise<any[]>;
    getDatabase(name: string): Promise<any | undefined>;
    syncSchema(databaseName: string): Promise<void>;
    syncTables(schema: string, fieldsByTable: Map<string, Set<string>>, timeout: number): Promise<void>;
    getTables(): Promise<any[]>;
    putTables(ids: ReadonlyArray<number>, params: any): Promise<void>;
    getQueryMetadata(tableId: number): Promise<any>;
    putFieldParams(id: number, params: FieldParams): Promise<void>;
    getCard(cardId: number): Promise<any>;
    getCards(collectionId?: number): Promise<any[]>;
    postCard(card: any): Promise<any>;
    putCard(id: number, card: any): Promise<any>;
    /** Removes leading, trailing and repeated slashes */
    private static normalizePath;
    getCollection(id: number): Promise<any | undefined>;
    /**
     * Lists collections under a given collection.
     * Returns a map from name to ID
     */
    getCollections(parentId?: number): Promise<Map<string, number>>;
    /** Gets the named path of a collection */
    getCollectionPath(collectionId: number): Promise<string>;
    /**
     * Gets a collection by its path. Each level of the collection tree should
     * contain collections with unique names, otherwise the return value is
     * not deterministic.
     */
    getCollectionByPath(path: string): Promise<any | undefined>;
    putCollection(id: number, collection: any): Promise<void>;
    postCollection(name: string, parentId?: number): Promise<number>;
    postCollectionPath(path: string): Promise<number>;
    getDashboards(): Promise<any[]>;
    getDashboard(id: number): Promise<any>;
    postDashboard(name: string, description: string, parameters: any[], collectionId: number): Promise<any>;
    postCardToDashboard(id: number, card: any): Promise<any>;
    dashboardBookmark(id: number): Promise<any>;
}
export {};
