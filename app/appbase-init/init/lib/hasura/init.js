"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.HasuraInit = void 0;
const axios_1 = __importDefault(require("axios"));
const commander_1 = require("commander");
const fs_extra_1 = __importDefault(require("fs-extra"));
const lodash_1 = require("lodash");
const path_1 = __importDefault(require("path"));
const pino_1 = __importDefault(require("pino"));
const pluralize_1 = __importDefault(require("pluralize"));
const verror_1 = require("verror");
const config_1 = require("../config");
const PermissionLoader_1 = require("./PermissionLoader");
const RESOURCES_DIR = path_1.default.join(config_1.BASE_RESOURCES_DIR, 'hasura');
class HasuraInit {
    constructor(api, logger, resourcesDir = RESOURCES_DIR) {
        this.api = api;
        this.logger = logger;
        this.resourcesDir = resourcesDir;
    }
    async listAllTables() {
        const response = await this.api.post('/v2/query', {
            type: 'run_sql',
            args: {
                source: 'default',
                sql: await fs_extra_1.default.readFile(path_1.default.join(this.resourcesDir, 'list-all-tables.sql'), 'utf8'),
                cascade: false,
                read_only: true,
            },
        });
        const result = (0, lodash_1.flatten)(response.data.result);
        return result.filter((table) => table !== 'tablename' && !table.startsWith('flyway_'));
    }
    async listAllForeignKeys() {
        const response = await this.api.post('/v2/query', {
            type: 'run_sql',
            args: {
                source: 'default',
                sql: await fs_extra_1.default.readFile(path_1.default.join(this.resourcesDir, 'list-all-foreign-keys.sql'), 'utf8'),
                cascade: false,
                read_only: true,
            },
        });
        const result = response.data.result;
        const foreignKeys = result
            .filter((row) => row.length === 3 && row[0] !== 'child_table' && !row[2].includes(','))
            .map((row) => {
            return {
                childTable: row[0],
                parentTable: row[1],
                column: row[2],
                relationshipNames: { object: row[1], array: (0, pluralize_1.default)(row[0]) },
            };
        });
        const conflictingKeys = (0, lodash_1.flatten)((0, lodash_1.values)((0, lodash_1.pickBy)((0, lodash_1.groupBy)(foreignKeys, (fk) => [fk.childTable, fk.parentTable]), (k) => k.length > 1)));
        const fixedKeys = (0, lodash_1.compact)(conflictingKeys.map((key) => {
            const modelParts = (0, lodash_1.compact)(key.parentTable.split('_'));
            if (!modelParts.length) {
                return undefined;
            }
            return {
                childTable: key.childTable,
                parentTable: key.parentTable,
                column: key.column,
                relationshipNames: {
                    object: `${key.parentTable}__${key.column}`,
                    array: `${(0, pluralize_1.default)(key.childTable)}By${(0, lodash_1.upperFirst)(key.column)}`,
                },
            };
        }));
        return (0, lodash_1.union)((0, lodash_1.difference)(foreignKeys, conflictingKeys), fixedKeys);
    }
    async getMetadata() {
        return await this.api
            .post('/v1/metadata', {
            type: 'export_metadata',
            version: 2,
            args: {},
        })
            .then((response) => response.data.metadata);
    }
    async getDbSource() {
        const metadata = await this.getMetadata();
        const sources = metadata.sources;
        const defaultSource = (0, lodash_1.find)(sources, (source) => source.name === 'default');
        if (!defaultSource) {
            throw new verror_1.VError('AppBase database not connected to Hasura');
        }
        return defaultSource;
    }
    /**
     * Dynamically creates the 'default' source in Hasura if it does not exist.
     * Uses the Hasura Metadata API (pg_add_source) with the provided database URL.
     * This is idempotent and safe to call if the source is missing.
     */
    async createDefaultSource(databaseUrl) {
        this.logger.info('Creating default data source in Hasura');
        await this.api.post('/v1/metadata', {
            type: 'pg_add_source',
            version: 1,
            args: {
                name: 'default',
                configuration: {
                    connection_info: {
                        database_url: databaseUrl,
                        use_prepared_statements: true,
                        isolation_level: 'read-committed',
                        pool_settings: {
                            connection_lifetime: 600,
                            retries: 1,
                            idle_timeout: 180,
                            max_connections: 50,
                        },
                    },
                },
            },
        });
        this.logger.info('Default data source created successfully');
    }
    async getQueryCollections() {
        return await this.getMetadata().then((metadata) => metadata.query_collections);
    }
    async getEndpoints() {
        return await this.getMetadata().then((metadata) => metadata.rest_endpoints);
    }
    async trackTable(table) {
        this.logger.debug('Adding %s table to Hasura schema', table);
        await this.api.post('/v1/metadata', {
            type: 'pg_track_table',
            args: {
                source: 'default',
                table,
                configuration: {},
            },
        });
    }
    async createObjectRelationship(fk) {
        this.logger.debug('Creating object relationship for %o', fk);
        await this.api.post('/v1/metadata', {
            type: 'pg_create_object_relationship',
            args: {
                table: fk.childTable,
                name: fk.relationshipNames.object,
                source: 'default',
                using: {
                    foreign_key_constraint_on: [fk.column],
                },
            },
        });
    }
    async createArrayRelationship(fk) {
        this.logger.debug('Creating array relationship for %o', fk);
        await this.api.post('/v1/metadata', {
            type: 'pg_create_array_relationship',
            args: {
                table: fk.parentTable,
                name: fk.relationshipNames.array,
                source: 'default',
                using: {
                    foreign_key_constraint_on: {
                        table: fk.childTable,
                        columns: [fk.column],
                    },
                },
            },
        });
    }
    // Permission Management Methods
    async createSelectPermission(table, role, permission) {
        var _a, _b;
        this.logger.debug('Creating select permission for table %s, role %s', table, role);
        try {
            await this.api.post('/v1/metadata', {
                type: 'pg_create_select_permission',
                args: {
                    table,
                    source: 'default',
                    role,
                    permission,
                },
            });
        }
        catch (error) {
            // Handle permission already exists error gracefully
            if (((_b = (_a = error.response) === null || _a === void 0 ? void 0 : _a.data) === null || _b === void 0 ? void 0 : _b.code) === 'already-exists') {
                this.logger.debug('Select permission already exists for table %s, role %s - skipping', table, role);
                return;
            }
            throw error;
        }
    }
    async createInsertPermission(table, role, permission) {
        var _a, _b;
        this.logger.debug('Creating insert permission for table %s, role %s', table, role);
        try {
            await this.api.post('/v1/metadata', {
                type: 'pg_create_insert_permission',
                args: {
                    table,
                    source: 'default',
                    role,
                    permission,
                },
            });
        }
        catch (error) {
            // Handle permission already exists error gracefully
            if (((_b = (_a = error.response) === null || _a === void 0 ? void 0 : _a.data) === null || _b === void 0 ? void 0 : _b.code) === 'already-exists') {
                this.logger.debug('Insert permission already exists for table %s, role %s - skipping', table, role);
                return;
            }
            throw error;
        }
    }
    async createUpdatePermission(table, role, permission) {
        var _a, _b;
        this.logger.debug('Creating update permission for table %s, role %s', table, role);
        try {
            await this.api.post('/v1/metadata', {
                type: 'pg_create_update_permission',
                args: {
                    table,
                    source: 'default',
                    role,
                    permission,
                },
            });
        }
        catch (error) {
            // Handle permission already exists error gracefully
            if (((_b = (_a = error.response) === null || _a === void 0 ? void 0 : _a.data) === null || _b === void 0 ? void 0 : _b.code) === 'already-exists') {
                this.logger.debug('Update permission already exists for table %s, role %s - skipping', table, role);
                return;
            }
            throw error;
        }
    }
    async createDeletePermission(table, role, permission) {
        var _a, _b;
        this.logger.debug('Creating delete permission for table %s, role %s', table, role);
        try {
            await this.api.post('/v1/metadata', {
                type: 'pg_create_delete_permission',
                args: {
                    table,
                    source: 'default',
                    role,
                    permission,
                },
            });
        }
        catch (error) {
            // Handle permission already exists error gracefully
            if (((_b = (_a = error.response) === null || _a === void 0 ? void 0 : _a.data) === null || _b === void 0 ? void 0 : _b.code) === 'already-exists') {
                this.logger.debug('Delete permission already exists for table %s, role %s - skipping', table, role);
                return;
            }
            throw error;
        }
    }
    async setupTablePermissions(table, role, config) {
        this.logger.info('Setting up permissions for table %s, role %s', table, role);
        if (config.select) {
            await this.createSelectPermission(table, role, config.select);
        }
        if (config.insert) {
            await this.createInsertPermission(table, role, config.insert);
        }
        if (config.update) {
            await this.createUpdatePermission(table, role, config.update);
        }
        if (config.delete) {
            await this.createDeletePermission(table, role, config.delete);
        }
    }
    async initializePermissionSystem() {
        this.logger.info('Initializing Hasura permission system with dynamic configuration');
        try {
            // Create permission loader with graceful fallback enabled
            const permissionLoader = new PermissionLoader_1.PermissionLoader(this.resourcesDir, {
                fallbackToDefaults: true,
                enableValidation: true
            }, this.logger);
            // Load permission configuration dynamically
            const permissionConfig = await permissionLoader.loadPermissions();
            // Apply permissions for each table and role
            for (const [tableName, tablePermissions] of Object.entries(permissionConfig.tables)) {
                this.logger.debug(`Configuring permissions for table: ${tableName}`);
                for (const [roleName, rolePermissions] of Object.entries(tablePermissions)) {
                    this.logger.debug(`Setting up permissions for role: ${roleName} on table: ${tableName}`);
                    await this.setupTablePermissions(tableName, roleName, rolePermissions);
                }
            }
            const tableCount = Object.keys(permissionConfig.tables).length;
            const totalRolePermissions = Object.values(permissionConfig.tables)
                .reduce((total, table) => total + Object.keys(table).length, 0);
            this.logger.info(`Permission system initialization complete - configured ${totalRolePermissions} role permissions across ${tableCount} tables`);
        }
        catch (error) {
            const errorMessage = error instanceof Error ? error.message : String(error);
            this.logger.error({ error: errorMessage }, 'Failed to initialize permission system');
            throw new verror_1.VError(error, 'Permission system initialization failed');
        }
    }
    async loadMetadata(metadata) {
        await this.api.post('/v1/metadata', {
            type: 'replace_metadata',
            version: 2,
            args: {
                allow_inconsistent_metadata: false,
                metadata,
            },
        });
    }
    static createSourceMetadata(tableNames, foreignKeys, databaseUrl) {
        const rels = {};
        for (const table of tableNames) {
            rels[table] = { objectRels: [], arrayRels: [] };
        }
        for (const fk of foreignKeys) {
            rels[fk.childTable].objectRels.push({
                name: fk.relationshipNames.object,
                using: { foreign_key_constraint_on: fk.column },
            });
            rels[fk.parentTable].arrayRels.push({
                name: fk.relationshipNames.array,
                using: {
                    foreign_key_constraint_on: {
                        column: fk.column,
                        table: { name: fk.childTable, schema: 'public' },
                    },
                },
            });
        }
        const tables = tableNames.map((name) => {
            return {
                table: { name, schema: 'public' },
                object_relationships: rels[name].objectRels,
                array_relationships: rels[name].arrayRels,
            };
        });
        const source = {
            name: 'default',
            kind: 'postgres',
            tables,
            configuration: {
                connection_info: {
                    use_prepared_statements: true,
                    database_url: databaseUrl || {
                        from_env: 'HASURA_GRAPHQL_DATABASE_URL',
                    },
                    isolation_level: 'read-committed',
                    pool_settings: {
                        connection_lifetime: 600,
                        retries: 1,
                        idle_timeout: 180,
                        max_connections: 50,
                    },
                },
            },
        };
        return source;
    }
    async loadQueryCollectionFromResources() {
        const directory = path_1.default.join(this.resourcesDir, 'endpoints');
        const mutations = [];
        await Promise.all(fs_extra_1.default
            .readdirSync(directory)
            .filter((file) => file.endsWith('.gql'))
            .map(async (file) => {
            const fileContents = await fs_extra_1.default.readFile(path_1.default.join(directory, file), 'utf8');
            mutations.push({
                // Remove the ".gql" from the file name and use as query name.
                name: file.substring(0, file.length - 4),
                query: fileContents,
            });
        }));
        return {
            name: 'allowed-queries',
            definition: {
                queries: mutations,
            },
        };
    }
    async addQueryToCollection(collectionName, query) {
        await this.api.post('/v1/metadata', {
            type: 'add_query_to_collection',
            args: {
                collection_name: collectionName,
                query_name: query.name,
                query: query.query,
            },
        });
    }
    async addEndpoint(endpoint) {
        await this.api.post('/v1/metadata', {
            type: 'create_rest_endpoint',
            args: endpoint,
        });
    }
    async updateQueryCollections(queryCollectionFromResources) {
        const queryCollections = await this.getQueryCollections();
        const toUpdate = (0, lodash_1.find)(queryCollections, (collection) => collection.name === queryCollectionFromResources.name);
        if (!toUpdate) {
            // The query collection from resources doesn't exist in the metadata.
            // Safely create a new query collection.
            this.logger.info('Creating query collection \'%s\'. %d queries added', queryCollectionFromResources.name, queryCollectionFromResources.definition.queries.length);
            await this.api.post('/v1/metadata', {
                type: 'create_query_collection',
                args: queryCollectionFromResources,
            });
            await this.api.post('/v1/metadata', {
                type: 'add_collection_to_allowlist',
                args: {
                    collection: queryCollectionFromResources.name,
                    scope: {
                        global: true,
                    },
                },
            });
        }
        else {
            const toAdd = [];
            for (const query of queryCollectionFromResources.definition.queries) {
                if (!(0, lodash_1.find)(toUpdate.definition.queries, (q) => q.name === query.name)) {
                    toAdd.push(query);
                }
            }
            if (toAdd.length > 0) {
                this.logger.info('Updating query collection \'%s\'. %d queries added.', queryCollectionFromResources.name, toAdd.length);
                await Promise.all(toAdd.map((query) => this.addQueryToCollection(toUpdate.name, query)));
            }
        }
    }
    async updateEndpoints(queryCollectionFromResources) {
        const endpoints = await this.getEndpoints();
        const endpointsFromResources = queryCollectionFromResources.definition.queries.map((q) => {
            return {
                name: q.name,
                url: q.name,
                comment: null,
                methods: ['POST'],
                definition: {
                    query: {
                        query_name: q.name,
                        collection_name: queryCollectionFromResources.name,
                    },
                },
            };
        });
        const toAdd = [];
        for (const endpoint of endpointsFromResources) {
            const endpointToUpdate = (0, lodash_1.find)(endpoints, (e) => e.name === endpoint.name);
            if (!endpointToUpdate) {
                toAdd.push(endpoint);
            }
        }
        if (toAdd.length > 0) {
            this.logger.info('Updating endpoints. %d added.', toAdd.length);
            await Promise.all(toAdd.map((endpoint) => this.addEndpoint(endpoint)));
        }
    }
    async createEndpoints() {
        const queryCollectionFromResources = await this.loadQueryCollectionFromResources();
        await this.updateQueryCollections(queryCollectionFromResources);
        await this.updateEndpoints(queryCollectionFromResources);
    }
    async trackAllTablesAndRelationships(databaseUrl) {
        var _a, _b, _c, _d;
        // First, ensure the default source exists before trying to query it
        let source = null;
        let trackedTables = [];
        try {
            source = await this.getDbSource();
            trackedTables = source.tables.filter((table) => table.table.schema === 'public');
        }
        catch (error) {
            this.logger.info('No default source found in Hasura metadata, creating one');
            if (!databaseUrl) {
                throw new verror_1.VError('Database URL is required to create default source');
            }
            await this.createDefaultSource(databaseUrl);
            // Now try to get the source again
            source = await this.getDbSource();
            trackedTables = source.tables.filter((table) => table.table.schema === 'public');
        }
        // Now that we have a source, we can safely query for tables and foreign keys
        const allTableNames = await this.listAllTables();
        const foreignKeys = await this.listAllForeignKeys();
        if (trackedTables.length === 0) {
            await this.loadMetadata({
                version: 3,
                sources: [
                    HasuraInit.createSourceMetadata(allTableNames, foreignKeys, databaseUrl),
                ],
            });
            // Attempt to find table names and foreign keys again if none were found
            // in the first pass.
            // See https://github.com/appbase-ai/appbase-community-edition/pull/81 for
            // more details.
            if (allTableNames.length === 0 && databaseUrl) {
                const tableNamesFromDbUrl = await this.listAllTables();
                const foreignKeysFromDbUrl = await this.listAllForeignKeys();
                await this.loadMetadata({
                    version: 3,
                    sources: [
                        HasuraInit.createSourceMetadata(tableNamesFromDbUrl, foreignKeysFromDbUrl, databaseUrl),
                    ],
                });
            }
            this.logger.info('Loaded source metadata into Hasura');
            return;
        }
        const untrackedTableNames = (0, lodash_1.difference)(allTableNames, trackedTables.map((table) => table.table.name));
        for (const table of untrackedTableNames) {
            await this.trackTable(table);
        }
        const relMap = {};
        let newObjectRels = 0;
        let newArrayRels = 0;
        // Only process relationships if we have a valid source
        if (source) {
            for (const table of trackedTables) {
                relMap[table.table.name] = {
                    objectRels: table.object_relationships,
                    arrayRels: table.array_relationships,
                };
            }
        }
        // Only create relationships if we have a valid source and tracked tables
        if (source && trackedTables.length > 0) {
            for (const fk of foreignKeys) {
                if (!(0, lodash_1.find)((_b = (_a = relMap[fk.childTable]) === null || _a === void 0 ? void 0 : _a.objectRels) !== null && _b !== void 0 ? _b : [], (rel) => rel.name === fk.relationshipNames.object &&
                    rel.using.foreign_key_constraint_on === fk.column)) {
                    await this.createObjectRelationship(fk);
                    newObjectRels++;
                }
                if (!(0, lodash_1.find)((_d = (_c = relMap[fk.parentTable]) === null || _c === void 0 ? void 0 : _c.arrayRels) !== null && _d !== void 0 ? _d : [], (rel) => rel.name === fk.relationshipNames.array &&
                    rel.using.foreign_key_constraint_on.column === fk.column)) {
                    await this.createArrayRelationship(fk);
                    newArrayRels++;
                }
            }
        }
        this.logger.info('Added %d tables to Hasura schema', untrackedTableNames.length);
        this.logger.info('Added %d object relationships to Hasura schema', newObjectRels);
        this.logger.info('Added %d array relationships to Hasura schema', newArrayRels);
    }
}
exports.HasuraInit = HasuraInit;
async function main() {
    commander_1.program
        .requiredOption('--hasura-url <string>')
        .option('--admin-secret <string>')
        .option('--database-url <string>');
    commander_1.program.parse();
    const options = commander_1.program.opts();
    const logger = (0, pino_1.default)({
        name: 'hasura-init',
        level: process.env.LOG_LEVEL || 'info',
    });
    const hasura = new HasuraInit(axios_1.default.create({
        baseURL: options.hasuraUrl,
        headers: {
            'X-Hasura-Role': 'admin',
            ...(options.adminSecret && {
                'X-Hasura-Admin-Secret': options.adminSecret,
            }),
        },
    }), logger);
    await hasura.trackAllTablesAndRelationships(options.databaseUrl);
    await hasura.createEndpoints();
    await hasura.initializePermissionSystem();
    logger.info('Hasura setup is complete');
}
if (require.main === module) {
    main().catch((err) => {
        console.error(err);
        process.exit(1);
    });
}
