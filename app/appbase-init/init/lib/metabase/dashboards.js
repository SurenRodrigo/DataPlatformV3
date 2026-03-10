"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.Dashboards = void 0;
const handlebars_1 = __importStar(require("handlebars"));
const lodash_1 = require("lodash");
const verror_1 = require("verror");
const config_1 = require("./config");
const metabase_1 = require("./metabase");
const DASHBOARD_REGEX = /\(\/dashboard\/(\d+)\)/g;
const REF_REGEX = /\\?"{{ (table|field|card|dashboard) \\{1,3}"([^\\"]+)\\{1,3}" }}\\?"/g;
const SYNC_TIMEOUT_MS = 100000;
class Dashboards {
    constructor(metabase, databaseId, logger) {
        this.metabase = metabase;
        this.databaseId = databaseId;
        this.logger = logger;
    }
    static async fromConfig(cfg) {
        if (!cfg.databaseName) {
            throw new verror_1.VError('no database name given');
        }
        const metabase = await metabase_1.Metabase.fromConfig(cfg.metabase);
        const database = await metabase.getDatabase(cfg.databaseName);
        const databaseId = database === null || database === void 0 ? void 0 : database.id;
        if (!databaseId) {
            throw new verror_1.VError('unable to find database: ' + cfg.databaseName);
        }
        return new Dashboards(metabase, databaseId, cfg.logger);
    }
    async getDatabaseMetadata() {
        const tables = await this.metabase.getTables();
        const metadata = await Promise.all(tables.map(async (table) => [
            table,
            await this.metabase.getQueryMetadata(table.id),
        ]));
        const tableIds = {};
        const fieldIds = {};
        const tableNames = {};
        const fieldNames = {};
        for (const [table, { fields }] of metadata) {
            if (table.db_id !== this.databaseId) {
                continue;
            }
            const tableName = Dashboards.tableName(table);
            tableIds[tableName] = table.id;
            tableNames[table.id] = tableName;
            for (const field of fields) {
                if (!fieldIds[tableName]) {
                    fieldIds[tableName] = {};
                }
                fieldIds[tableName][field.name] = field.id;
                fieldNames[field.id] = `${tableName}.${field.name}`;
            }
        }
        return { tableIds, fieldIds, tableNames, fieldNames };
    }
    /** Exports a dashboard to a template string */
    async export(dashboardOrId) {
        var _a, _b, _c;
        const dashboard = typeof dashboardOrId === 'number'
            ? await this.metabase.getDashboard(dashboardOrId)
            : dashboardOrId;
        const name = dashboard.name;
        const description = (_a = dashboard.description) !== null && _a !== void 0 ? _a : undefined;
        const { tableNames, fieldNames } = await this.getDatabaseMetadata();
        const cardNames = {};
        const parentCardById = new Map();
        for (const cardLayout of dashboard.ordered_cards) {
            const cardId = cardLayout.card_id;
            if (cardId) {
                cardNames[cardId] = cardLayout.card.name;
            }
            // If this card references another card, then add it
            const parentCardId = Dashboards.parentCardId(cardLayout.card);
            if (parentCardId && !parentCardById.has(parentCardId)) {
                const parentCard = await this.metabase.getCard(parentCardId);
                parentCardById.set(parentCardId, parentCard);
                cardNames[parentCardId] = parentCard.name;
            }
            for (const seriesLayout of cardLayout.series) {
                const seriesCardId = seriesLayout.id;
                if (!seriesCardId) {
                    throw new verror_1.VError('Series does not have an id');
                }
                if (seriesCardId) {
                    cardNames[seriesCardId] = seriesLayout.name;
                }
            }
        }
        const dashboards = await this.metabase.getDashboards();
        const dashboardNames = {};
        for (const item of dashboards) {
            const dashboardId = item.id;
            dashboardNames[dashboardId] = item.name;
        }
        this.templatize(dashboard, tableNames, fieldNames, cardNames, dashboardNames);
        const cards = [];
        const layout = [];
        const parameters = dashboard.parameters;
        const toCard = (card) => ({
            name: card.name,
            description: card.description,
            display: card.display,
            table_id: card.table_id,
            dataset_query: card.dataset_query,
            visualization_settings: (0, lodash_1.omit)(card.visualization_settings, 'click_behavior'),
        });
        // Add all source cards first...
        for (const parentCard of parentCardById.values()) {
            // Since source cards are not part of the dashboard
            // we need to templatize them separately
            this.templatize(parentCard, tableNames, fieldNames, cardNames, dashboardNames);
            cards.push(toCard(parentCard));
        }
        // ...then add cards in the dashboard
        for (const cardLayout of dashboard.ordered_cards) {
            if (cardLayout.card_id) {
                cards.push(toCard(cardLayout.card));
            }
            const series = [];
            for (const seriesLayout of cardLayout.series) {
                if (seriesLayout.id) {
                    cards.push(toCard(seriesLayout));
                }
                series.push({ id: `{{ card "${seriesLayout.name}" }}` });
            }
            layout.push({
                row: cardLayout.row,
                col: cardLayout.col,
                // Support for v45+
                sizeX: (_b = cardLayout.sizeX) !== null && _b !== void 0 ? _b : cardLayout.size_x,
                sizeY: (_c = cardLayout.sizeY) !== null && _c !== void 0 ? _c : cardLayout.size_y,
                card_id: cardLayout.card_id,
                series,
                parameter_mappings: cardLayout.parameter_mappings,
                visualization_settings: cardLayout.visualization_settings,
            });
        }
        const collectionId = dashboard.collection_id;
        const path = await this.metabase.getCollectionPath(collectionId);
        // Unstringify references since they'll be populated with numbers
        const json = JSON.stringify({
            name,
            description,
            cards,
            parameters,
            layout,
            path,
        }, null, 2);
        return json.replace(REF_REGEX, '{{ $1 "$2" }}') + '\n';
    }
    /** Recursive function to replace ids with named references */
    templatize(cfg, tableNames, fieldNames, cardNames, dashboardNames) {
        var _a;
        if (!cfg) {
            return;
        }
        else if ((0, lodash_1.isPlainObject)(cfg)) {
            // eslint-disable-next-line prefer-const
            for (let [key, val] of Object.entries(cfg)) {
                if (Dashboards.isJSON(key)) {
                    const jsonKey = JSON.parse(key);
                    this.templatize(jsonKey, tableNames, fieldNames, cardNames, dashboardNames);
                    delete cfg[key];
                    key = JSON.stringify(jsonKey);
                    cfg[key] = val;
                }
                if ((0, lodash_1.isNumber)(val)) {
                    if (key === 'table_id' || key === 'source-table') {
                        cfg[key] = `{{ table "${tableNames[val]}" }}`;
                    }
                    else if (key === 'card_id') {
                        cfg[key] = `{{ card "${cardNames[val]}" }}`;
                    }
                    else if (key === 'database' || key === 'database_id') {
                        // Not needed. Database will be populated on import.
                        cfg[key] = undefined;
                    }
                    else if (key === 'targetId') {
                        // Custom logic for click_behavior when key is targetId
                        if (cfg.linkType === 'dashboard') {
                            cfg[key] = `{{ dashboard "${dashboardNames[val]}" }}`;
                        }
                    }
                    else if (key === 'id') {
                        if ((_a = cfg.dataset_query) === null || _a === void 0 ? void 0 : _a.query) {
                            cfg[key] = `{{ card "${cardNames[val]}" }}`;
                        }
                    }
                }
                else if ((0, lodash_1.isString)(val)) {
                    if (key === 'query') {
                        cfg[key] = val.replace(/{{/g, '<<').replace(/}}/g, '>>');
                    }
                    else if (key === 'source-table') {
                        const cardId = Dashboards.parentCardId(val);
                        if (!cardId) {
                            throw new verror_1.VError('unable to extract parent card from: %s', val);
                        }
                        cfg[key] = `{{ card "${cardNames[cardId]}" }}`;
                    }
                    else if (key === 'text') {
                        cfg[key] = val.replace(DASHBOARD_REGEX, (match, p1) => `(/dashboard/{{ dashboard '${dashboardNames[Number(p1)]}' }})`);
                    }
                }
                else {
                    this.templatize(val, tableNames, fieldNames, cardNames, dashboardNames);
                }
            }
        }
        else if (Array.isArray(cfg)) {
            if (!(cfg === null || cfg === void 0 ? void 0 : cfg.length)) {
                return;
            }
            else if (cfg[0] === 'field' && (0, lodash_1.isNumber)(cfg[1])) {
                const fieldName = fieldNames[cfg[1]];
                if (!fieldName) {
                    throw new verror_1.VError('unknown field id: ' + cfg[1]);
                }
                cfg[1] = `{{ field "${fieldName}" }}`;
                if (cfg[2] && typeof cfg[2] === 'object' && cfg[2]['source-field']) {
                    const sourceFieldName = fieldNames[cfg[2]['source-field']];
                    if (!sourceFieldName) {
                        throw new verror_1.VError('unknown field id: ' + cfg[2]['source-field']);
                    }
                    cfg[2]['source-field'] = `{{ field "${sourceFieldName}" }}`;
                }
            }
            for (const entry of cfg) {
                this.templatize(entry, tableNames, fieldNames, cardNames, dashboardNames);
            }
        }
    }
    /**
     * Sync database metadata based on tables and fields referenced
     * in dashboard templates
     */
    async syncTemplateMetadata(templates) {
        var _a;
        const fieldsByTable = new Map();
        for (const template of templates) {
            for (const [table, fields] of this.getFields(template)) {
                if (!fieldsByTable.has(table)) {
                    fieldsByTable.set(table, new Set());
                }
                for (const field of fields) {
                    (_a = fieldsByTable.get(table)) === null || _a === void 0 ? void 0 : _a.add(field);
                }
            }
        }
        this.logger.info('Syncing tables in Metabase: %s', [...fieldsByTable.keys()].map((table) => table));
        await this.metabase.syncTables('public', fieldsByTable, SYNC_TIMEOUT_MS);
        this.logger.info('Finished syncing tables in Metabase');
        return await this.getDatabaseMetadata();
    }
    /** Get fields referenced in template by name */
    getFields(template) {
        const fieldsByTable = new Map();
        const handlebars = handlebars_1.default.create();
        handlebars.registerHelper('table', (table) => {
            if (!fieldsByTable.has(table)) {
                fieldsByTable.set(table, new Set());
            }
            return 0;
        });
        handlebars.registerHelper('field', (name) => {
            var _a;
            const split = name.split('.');
            if (split.length !== 2) {
                throw new verror_1.VError('invalid field: ' + name);
            }
            const table = split[0];
            const field = split[1];
            if (!fieldsByTable.has(table)) {
                fieldsByTable.set(table, new Set());
            }
            (_a = fieldsByTable.get(table)) === null || _a === void 0 ? void 0 : _a.add(field);
            return 0;
        });
        handlebars.registerHelper('card', () => {
            return 0;
        });
        handlebars.registerHelper('dashboard', () => {
            return 0;
        });
        handlebars.compile(template)({});
        return fieldsByTable;
    }
    /** Returns a map from card name to its parent card name */
    getCardDependencies(template) {
        var _a, _b;
        const handlebars = handlebars_1.default.create();
        handlebars.registerHelper('table', () => 0);
        handlebars.registerHelper('field', () => 0);
        handlebars.registerHelper('dashboard', () => 0);
        handlebars.registerHelper('card', (name) => new handlebars_1.SafeString(`"${name}"`));
        const dashboard = JSON.parse(handlebars.compile(template)({}));
        const cardDependencies = new Map();
        for (const card of dashboard.cards) {
            const sourceTable = (_b = (_a = card.dataset_query) === null || _a === void 0 ? void 0 : _a.query) === null || _b === void 0 ? void 0 : _b['source-table'];
            if ((0, lodash_1.isString)(sourceTable)) {
                cardDependencies.set(card.name, sourceTable);
                if (cardDependencies.has(sourceTable)) {
                    throw new verror_1.VError('cards referenced by other cards must be based on tables, ' +
                        // eslint-disable-next-line @typescript-eslint/quotes
                        `but card '%s', which is referenced by card '%s', is not`, sourceTable, card.name);
                }
            }
        }
        return cardDependencies;
    }
    renderTemplate(template, tableIds, fieldIds, cardIds, dashboardIds) {
        const handlebars = handlebars_1.default.create();
        handlebars.registerHelper('table', (name) => {
            const id = tableIds[name];
            if (id) {
                return typeof id === 'string' ? new handlebars_1.SafeString(`"${id}"`) : id;
            }
            throw new verror_1.VError('unknown table: ' + name);
        });
        handlebars.registerHelper('field', (name) => {
            var _a, _b;
            const split = name.split('.');
            if (split.length !== 2) {
                throw new verror_1.VError('invalid field: ' + name);
            }
            const table = split[0];
            const field = split[1];
            if ((_a = fieldIds[table]) === null || _a === void 0 ? void 0 : _a[field]) {
                return (_b = fieldIds[table]) === null || _b === void 0 ? void 0 : _b[field];
            }
            throw new verror_1.VError('unknown field: ' + name);
        });
        handlebars.registerHelper('card', (name) => {
            if (cardIds) {
                if (cardIds[name]) {
                    return cardIds[name];
                }
                throw new verror_1.VError('unknown card: ' + name);
            }
            return 'null';
        });
        handlebars.registerHelper('dashboard', (name) => {
            if (dashboardIds) {
                if (dashboardIds[name]) {
                    return dashboardIds[name];
                }
                throw new verror_1.VError('unknown dashboard: ' + name);
            }
            return 'null';
        });
        return JSON.parse(JSON.stringify(JSON.parse(handlebars.compile(template)({})), (key, value) => {
            // ->> and #>> must not be replaced
            // https://www.postgresql.org/docs/9.4/functions-json.html
            return key === 'query' && (0, lodash_1.isString)(value)
                ? value.replace(/<</g, '{{').replace(/([^-#])>>/g, '$1}}')
                : value;
        }));
    }
    async importNew() {
        const dashboards = await (0, config_1.loadDashboards)();
        const existingDashboards = await this.metabase.getDashboards();
        const existingDashboardNames = new Set();
        for (const item of existingDashboards) {
            existingDashboardNames.add(item.name);
            this.logger.debug('existing dashboard: %s', item.name);
        }
        // we need to quickly parse the json to access the dashboard name
        // we simply ignore missing helpers
        const handlebars = handlebars_1.default.create();
        handlebars.registerHelper('helperMissing', function () {
            return new handlebars_1.default.SafeString('null');
        });
        const newDashboards = dashboards.filter((dashboard) => !existingDashboardNames.has(JSON.parse(handlebars.compile(dashboard.template)({})).name));
        return await this.import(newDashboards);
    }
    async importOne(name) {
        const dashboards = new Array(await (0, config_1.loadDashboard)(name));
        return await this.import(dashboards);
    }
    async import(dashboards) {
        this.logger.info('Importing dashboards: %s', dashboards.map((d) => d.name));
        const templates = dashboards.map((d) => d.template);
        const { tableIds, fieldIds } = await this.syncTemplateMetadata(templates);
        const cards = {};
        const dashboardIds = {};
        const collectionIds = {};
        const updatedFieldIds = new Set();
        const templatesByName = {};
        const renderedTemplates = [];
        for (const template of templates) {
            const cfg = this.renderTemplate(template, tableIds, fieldIds);
            renderedTemplates.push(cfg);
            templatesByName[cfg.name] = template;
        }
        for (const cfg of (0, lodash_1.sortBy)(renderedTemplates, (t) => t.priority)) {
            for (const { field, type } of cfg.fields || []) {
                if (!updatedFieldIds.has(field)) {
                    const param = { semantic_type: type };
                    await this.metabase.putFieldParams(field, param);
                    updatedFieldIds.add(field);
                }
            }
            const collectionId = await this.metabase.postCollectionPath(cfg.path);
            const { id } = await this.metabase.postDashboard(cfg.name, cfg.description, cfg.parameters, collectionId);
            cards[cfg.name] = cfg.cards;
            collectionIds[cfg.name] = collectionId;
            dashboardIds[cfg.name] = id;
            if (cfg.bookmark) {
                await this.metabase.dashboardBookmark(id);
            }
        }
        this.logger.info('Created empty dashboards');
        // Second pass on the templates now that we have card and dashboard ids
        for (const [name, template] of Object.entries(templatesByName)) {
            const cardDependencies = this.getCardDependencies(template);
            const parentCardNames = new Set(cardDependencies.values());
            const parentCards = [];
            const otherCards = [];
            for (const card of cards[name]) {
                if (parentCardNames.has(card.name)) {
                    parentCards.push(card);
                }
                else {
                    otherCards.push(card);
                }
            }
            const parentCardIds = await this.createCards(collectionIds[name], parentCards, true);
            // Replace empty references to parent cards with parent card id
            for (const card of otherCards) {
                const parentCardName = cardDependencies.get(card.name);
                if (parentCardName) {
                    const parentCardId = parentCardIds[parentCardName];
                    if (!parentCardId) {
                        throw new verror_1.VError(
                        // eslint-disable-next-line @typescript-eslint/quotes
                        `unable to find parent card '%s' of card '%s'`, parentCardName, card.name);
                    }
                    const cardRef = `card__${parentCardId}`;
                    card.dataset_query.query['source-table'] = cardRef;
                }
            }
            const otherCardIds = await this.createCards(collectionIds[name], otherCards, true);
            const cardIds = { ...parentCardIds, ...otherCardIds };
            const cfg = this.renderTemplate(template, tableIds, fieldIds, cardIds, dashboardIds);
            const id = dashboardIds[cfg.name];
            for (const card of cfg.layout) {
                await this.metabase.postCardToDashboard(id, card);
            }
        }
    }
    async createCards(collectionId, cards, upsert) {
        this.logger.info('Creating %d cards in collection %s', cards.length, collectionId);
        const existingCardsByName = {};
        if (upsert) {
            const existingCards = await this.metabase.getCards(collectionId);
            existingCards.sort((c1, c2) => {
                // Sort cards from least to most recent so that the most
                // recent card with a name is chosen in the forEach
                const d1 = new Date(c1.created_at);
                const d2 = new Date(c2.created_at);
                return d1.getTime() - d2.getTime();
            });
            for (const card of existingCards) {
                existingCardsByName[card.name] = {
                    id: card.id,
                    name: card.name,
                    description: card.description,
                    display: card.display,
                    collection_id: card.collection_id,
                    database_id: card.database_id,
                    table_id: card.table_id,
                    dataset_query: card.dataset_query,
                    visualization_settings: card.visualization_settings,
                };
            }
        }
        // Deduplicate cards by name
        const cardsByName = {};
        for (const card of cards) {
            cardsByName[card.name] = card;
        }
        const cardIds = {};
        let createdOrUpdated = 0;
        for (const card of Object.values(cardsByName)) {
            card.collection_id = collectionId;
            card.database_id = this.databaseId;
            card.dataset_query.database = this.databaseId;
            const existingCard = existingCardsByName[card.name];
            if (existingCard && upsert) {
                if (!Dashboards.equalCards(card, existingCard)) {
                    await this.metabase.putCard(existingCard.id, card);
                    createdOrUpdated++;
                }
                cardIds[card.name] = existingCard.id;
            }
            else {
                const { id } = await this.metabase.postCard(card);
                cardIds[card.name] = id;
                createdOrUpdated++;
            }
        }
        this.logger.info('Created or updated %d out of %d cards in collection %s', createdOrUpdated, cards.length, collectionId);
        return cardIds;
    }
    async getOrCreateCollection(path) {
        const collection = await this.metabase.getCollectionByPath(path);
        if (collection) {
            return collection.id;
        }
        return await this.metabase.postCollectionPath(path);
    }
    static parentCardId(cardOrRef) {
        var _a, _b;
        const cardRef = !(0, lodash_1.isString)(cardOrRef)
            ? (_b = (_a = cardOrRef.dataset_query) === null || _a === void 0 ? void 0 : _a.query) === null || _b === void 0 ? void 0 : _b['source-table']
            : cardOrRef;
        if (!(0, lodash_1.isString)(cardRef) || !cardRef.startsWith('card__')) {
            return undefined;
        }
        return parseInt(cardRef.replace(/^card__/, ''), 10);
    }
    static tableName(table) {
        if (typeof table === 'string') {
            return table;
        }
        return table.name;
    }
    static isJSON(str) {
        try {
            JSON.parse(str);
        }
        catch (e) {
            return false;
        }
        return true;
    }
    static equalCards(card1, card2) {
        // series cards lack a table_id on export
        const predicate = (v, k) => k === 'id' || k === 'table_id' || !v;
        return (0, lodash_1.isEqual)((0, lodash_1.omitBy)(card1, predicate), (0, lodash_1.omitBy)(card2, predicate));
    }
}
exports.Dashboards = Dashboards;
