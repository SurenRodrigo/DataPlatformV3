"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.Metabase = void 0;
exports.wrapApiError = wrapApiError;
const axios_1 = __importDefault(require("axios"));
const verror_1 = require("verror");
function wrapApiError(cause, msg) {
    // Omit verbose axios error
    const truncated = new verror_1.VError(cause.message);
    return new verror_1.VError(truncated, msg);
}
// 12 hours
const SYNC_POLL_MILLIS = 2000;
class Metabase {
    constructor(api) {
        this.api = api;
    }
    static async fromConfig(cfg) {
        const token = await Metabase.sessionToken(cfg);
        const api = axios_1.default.create({
            baseURL: `${cfg.url}/api`,
            headers: {
                'X-Metabase-Session': token,
            },
        });
        return new Metabase(api);
    }
    static async sessionToken(cfg) {
        const { url, username, password } = cfg;
        try {
            const { data } = await axios_1.default.post(`${url}/api/session`, {
                username,
                password,
            });
            return data.id;
        }
        catch (err) {
            throw wrapApiError(err, 'failed to get session token');
        }
    }
    async getDatabases() {
        try {
            const { data } = await this.api.get('database');
            return Array.isArray(data) ? data : (data === null || data === void 0 ? void 0 : data.data) || [];
        }
        catch (err) {
            throw wrapApiError(err, 'unable to get databases');
        }
    }
    async getDatabase(name) {
        try {
            const dbs = await this.getDatabases();
            return dbs.find((d) => { var _a; return ((_a = d === null || d === void 0 ? void 0 : d.details) === null || _a === void 0 ? void 0 : _a.dbname) === name; });
        }
        catch (err) {
            throw wrapApiError(err, 'unable to find database: ' + name);
        }
    }
    async syncSchema(databaseName) {
        const db = await this.getDatabase(databaseName);
        if (!db) {
            throw new verror_1.VError('unable to find database: ' + databaseName);
        }
        await this.api.post(`database/${db.id}/sync_schema`);
    }
    async syncTables(schema, fieldsByTable, timeout) {
        const dbs = await this.getDatabases();
        for (const db of dbs) {
            try {
                await this.api.post(`database/${db.id}/sync_schema`);
            }
            catch (err) {
                throw wrapApiError(err, `failed to sync database ${db.id}`);
            }
        }
        const checkSync = async (allTables) => {
            var _a;
            const tables = allTables.filter((t) => {
                return t.schema === schema && fieldsByTable.has(t.name);
            });
            // First check all tables are synced
            if (tables.length < fieldsByTable.size) {
                return false;
            }
            // Next check all fields of each table are synced
            for (const table of tables) {
                const metadata = await this.getQueryMetadata(table.id);
                const actualFields = new Set((_a = metadata === null || metadata === void 0 ? void 0 : metadata.fields) === null || _a === void 0 ? void 0 : _a.map((f) => f.name));
                for (const field of fieldsByTable.get(table.name) || []) {
                    if (!actualFields.has(field)) {
                        return false;
                    }
                }
            }
            return true;
        };
        let isSynced = await checkSync(await this.getTables());
        const deadline = Date.now() + timeout;
        while (!isSynced && Date.now() < deadline) {
            await new Promise((resolve) => {
                setTimeout(resolve, SYNC_POLL_MILLIS);
                return;
            });
            try {
                isSynced = await checkSync(await this.getTables());
            }
            catch (err) {
                throw wrapApiError(err, 'failed to get tables');
            }
        }
        if (!isSynced) {
            throw new verror_1.VError('failed to sync tables %s within timeout: %s ms', Array.from(fieldsByTable.keys()), timeout);
        }
    }
    async getTables() {
        try {
            const { data } = await this.api.get('table');
            return data;
        }
        catch (err) {
            throw wrapApiError(err, 'unable to get tables');
        }
    }
    // eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
    async putTables(ids, params) {
        try {
            if (ids.length) {
                await this.api.put('table', { ids, ...params });
            }
        }
        catch (err) {
            throw wrapApiError(err, `unable to put tables: ${ids}`);
        }
    }
    async getQueryMetadata(tableId) {
        try {
            const { data } = await this.api.get(`table/${tableId}/query_metadata`, {
                params: { include_sensitive_fields: true },
            });
            return data;
        }
        catch (err) {
            throw wrapApiError(err, 'unable to get metadata for table: ' + tableId);
        }
    }
    async putFieldParams(id, params) {
        try {
            await this.api.put(`field/${id}`, params);
        }
        catch (err) {
            throw wrapApiError(err, `unable to set field params of ${id} to ${params}`);
        }
    }
    async getCard(cardId) {
        try {
            const { data } = await this.api.get(`card/${cardId}`);
            return data;
        }
        catch (err) {
            throw wrapApiError(err, 'unable to get card: ' + cardId);
        }
    }
    async getCards(collectionId) {
        try {
            const { data } = await this.api.get('card');
            return data.filter((c) => !collectionId || c.collection_id === collectionId);
        }
        catch (err) {
            throw wrapApiError(err, 'unable to get cards');
        }
    }
    // eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
    async postCard(card) {
        try {
            const { data } = await this.api.post('card', card);
            return data;
        }
        catch (err) {
            throw wrapApiError(err, 'unable to post card');
        }
    }
    // eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
    async putCard(id, card) {
        var _a;
        try {
            card.description = (_a = card.description) !== null && _a !== void 0 ? _a : null;
            const { data } = await this.api.put(`card/${id}`, card);
            return data;
        }
        catch (err) {
            throw wrapApiError(err, `unable to put card: ${id}`);
        }
    }
    /** Removes leading, trailing and repeated slashes */
    static normalizePath(path) {
        return path.replace(/^\/+/, '').replace(/\/+$/, '').replace(/\/\/+/, '/');
    }
    async getCollection(id) {
        try {
            const { data } = await this.api.get(`collection/${id}`);
            return data;
        }
        catch (err) {
            throw wrapApiError(err, 'unable to get collection with id: ' + id);
        }
    }
    /**
     * Lists collections under a given collection.
     * Returns a map from name to ID
     */
    async getCollections(parentId) {
        const id = parentId !== null && parentId !== void 0 ? parentId : 'root';
        try {
            const { data } = await this.api.get(`collection/${id}/items`, {
                params: { models: 'collection' },
            });
            const collections = new Map();
            for (const collection of data.data || []) {
                collections.set(collection.name, collection.id);
            }
            return collections;
        }
        catch (err) {
            throw wrapApiError(err, `unable to list collections under ${id}`);
        }
    }
    /** Gets the named path of a collection */
    async getCollectionPath(collectionId) {
        var _a;
        const collection = await this.getCollection(collectionId);
        const ancestorById = new Map();
        for (const ancestor of collection.effective_ancestors || []) {
            ancestorById.set(ancestor.id, ancestor.name);
        }
        const ancestors = [];
        for (const dir of ((_a = collection.effective_location) === null || _a === void 0 ? void 0 : _a.split('/')) || []) {
            // Skip empty directories caused by extra slashes
            if (!dir) {
                continue;
            }
            const id = parseInt(dir, 10);
            if (isNaN(id)) {
                throw new verror_1.VError('collection %d has invalid dir %s in location: %s', collectionId, dir, collection.effective_location);
            }
            const ancestor = ancestorById.get(id);
            if (!ancestor) {
                throw new verror_1.VError('collection %d has unknown ancestor: %d', collectionId, id);
            }
            ancestors.push(ancestor);
        }
        ancestors.push(collection.name);
        return '/' + ancestors.join('/');
    }
    /**
     * Gets a collection by its path. Each level of the collection tree should
     * contain collections with unique names, otherwise the return value is
     * not deterministic.
     */
    async getCollectionByPath(path) {
        let parentId;
        for (const dir of Metabase.normalizePath(path).split('/')) {
            const collections = await this.getCollections(parentId);
            const collectionId = collections.get(dir);
            if (!collectionId) {
                return undefined;
            }
            parentId = collectionId;
        }
        return parentId ? await this.getCollection(parentId) : undefined;
    }
    // eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
    async putCollection(id, collection) {
        try {
            await this.api.put(`collection/${id}`, collection);
        }
        catch (err) {
            throw wrapApiError(err, 'unable to put collection: ' + id);
        }
    }
    async postCollection(name, parentId) {
        try {
            const body = { name, parent_id: parentId, color: '#9370DB' };
            const { data } = await this.api.post('collection', body);
            return data.id;
        }
        catch (err) {
            throw wrapApiError(err, 'unable to post collection: ' + name);
        }
    }
    async postCollectionPath(path) {
        let parentId;
        for (const dir of Metabase.normalizePath(path).split('/') || []) {
            const collections = await this.getCollections(parentId);
            let collectionId = collections.get(dir);
            if (!collectionId) {
                collectionId = await this.postCollection(dir, parentId);
            }
            parentId = collectionId;
        }
        if (!parentId) {
            throw new Error('path must contain at least one collection');
        }
        return parentId;
    }
    async getDashboards() {
        try {
            const { data } = await this.api.get('dashboard');
            return data;
        }
        catch (err) {
            throw wrapApiError(err, 'unable to get dashboards');
        }
    }
    async getDashboard(id) {
        try {
            const { data } = await this.api.get(`dashboard/${id}`);
            return data;
        }
        catch (err) {
            throw wrapApiError(err, 'unable to get dashboard: ' + id);
        }
    }
    async postDashboard(name, description, parameters, collectionId) {
        try {
            const { data } = await this.api.post('dashboard', {
                name,
                description: description !== null && description !== void 0 ? description : null,
                parameters,
                collection_id: collectionId,
            });
            return data;
        }
        catch (err) {
            throw wrapApiError(err, 'unable to post dashboard: ' + name);
        }
    }
    // eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
    async postCardToDashboard(id, card) {
        var _a;
        try {
            const { data } = await this.api.post(`dashboard/${id}/cards`, {
                ...card,
                // Accept cardId or card_id
                cardId: (_a = card.cardId) !== null && _a !== void 0 ? _a : card.card_id,
                // v45+ size options. For v44 and below Metabase ignores them
                size_x: card.sizeX,
                size_y: card.sizeY,
            });
            return data;
        }
        catch (err) {
            //NB: for AppBase, Throwing an error for card creation is not needed. 
            const error = wrapApiError(err, 'unable to add card to dashboard: ' + id);
            console.log(JSON.stringify(error, null, 2));
        }
    }
    async dashboardBookmark(id) {
        try {
            const { data } = await this.api.post(`bookmark/dashboard/${id}`);
            return data;
        }
        catch (err) {
            throw wrapApiError(err, 'unable to bookmark dashboard: ' + id);
        }
    }
}
exports.Metabase = Metabase;
