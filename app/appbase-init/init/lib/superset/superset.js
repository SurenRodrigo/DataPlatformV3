"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.Superset = void 0;
exports.wrapApiError = wrapApiError;
const axios_1 = __importDefault(require("axios"));
const verror_1 = require("verror");
function wrapApiError(cause, msg) {
    // Omit verbose axios error
    const truncated = new verror_1.VError(cause.message);
    return new verror_1.VError(truncated, msg);
}
class Superset {
    constructor(api, refreshToken) {
        this.api = api;
        this.refreshToken = refreshToken;
    }
    static async fromConfig(cfg) {
        const tokens = await Superset.authenticate(cfg);
        const api = axios_1.default.create({
            baseURL: `${cfg.url}/api/v1`,
            headers: {
                'Authorization': `Bearer ${tokens.access_token}`,
                'Content-Type': 'application/json',
            },
        });
        return new Superset(api, tokens.refresh_token);
    }
    static async authenticate(cfg) {
        const { url, username, password } = cfg;
        try {
            const { data } = await axios_1.default.post(`${url}/api/v1/security/login`, {
                username,
                password,
                provider: 'db',
                refresh: true
            });
            return {
                access_token: data.access_token,
                refresh_token: data.refresh_token
            };
        }
        catch (err) {
            throw wrapApiError(err, 'failed to authenticate with Superset');
        }
    }
    async refreshAccessToken() {
        var _a;
        try {
            const { data } = await axios_1.default.post(`${(_a = this.api.defaults.baseURL) === null || _a === void 0 ? void 0 : _a.replace('/api/v1', '')}/api/v1/security/refresh`, {
                refresh_token: this.refreshToken
            });
            this.api.defaults.headers['Authorization'] = `Bearer ${data.access_token}`;
        }
        catch (err) {
            throw wrapApiError(err, 'failed to refresh access token');
        }
    }
    async getDatabases() {
        try {
            const { data } = await this.api.get('database/');
            return data.result || [];
        }
        catch (err) {
            throw wrapApiError(err, 'unable to get databases');
        }
    }
    async getDatabase(name) {
        try {
            const databases = await this.getDatabases();
            return databases.find((db) => (db === null || db === void 0 ? void 0 : db.database_name) === name);
        }
        catch (err) {
            throw wrapApiError(err, 'unable to find database: ' + name);
        }
    }
    async createDatabase(config) {
        try {
            const { data } = await this.api.post('database/', config);
            return data;
        }
        catch (err) {
            throw wrapApiError(err, `failed to create database: ${config.database_name}`);
        }
    }
    async updateDatabase(id, updates) {
        try {
            const { data } = await this.api.put(`database/${id}`, updates);
            return data;
        }
        catch (err) {
            throw wrapApiError(err, `failed to update database: ${id}`);
        }
    }
    async testDatabaseConnection(config) {
        try {
            const { data } = await this.api.post('database/test_connection', config);
            return data.message === 'OK';
        }
        catch (err) {
            throw wrapApiError(err, 'failed to test database connection');
        }
    }
    async syncDatabase(databaseId) {
        try {
            // Get all datasets for this database and sync each one
            const datasets = await this.getDatasets();
            const databaseDatasets = datasets.filter((dataset) => { var _a; return ((_a = dataset.database) === null || _a === void 0 ? void 0 : _a.id) === databaseId; });
            const syncPromises = databaseDatasets.map((dataset) => this.syncDataset(dataset.id));
            await Promise.all(syncPromises);
        }
        catch (err) {
            throw wrapApiError(err, `failed to sync database ${databaseId}`);
        }
    }
    async syncDataset(datasetId) {
        try {
            await this.api.put(`dataset/${datasetId}/refresh`);
        }
        catch (err) {
            throw wrapApiError(err, `failed to sync dataset ${datasetId}`);
        }
    }
    async getDatasetByName(databaseName, tableName) {
        try {
            const datasets = await this.getDatasets();
            return datasets.find((dataset) => {
                var _a, _b;
                if (tableName) {
                    return ((_a = dataset.database) === null || _a === void 0 ? void 0 : _a.database_name) === databaseName &&
                        dataset.table_name === tableName;
                }
                return ((_b = dataset.database) === null || _b === void 0 ? void 0 : _b.database_name) === databaseName;
            });
        }
        catch (err) {
            throw wrapApiError(err, `unable to find dataset for database: ${databaseName}`);
        }
    }
    async getTables(databaseId) {
        try {
            const { data } = await this.api.get(`database/${databaseId}/tables/`);
            return data.result || [];
        }
        catch (err) {
            throw wrapApiError(err, `unable to get tables for database ${databaseId}`);
        }
    }
    async getDashboards() {
        try {
            const { data } = await this.api.get('dashboard/');
            return data.result || [];
        }
        catch (err) {
            throw wrapApiError(err, 'unable to get dashboards');
        }
    }
    async getDashboard(id) {
        try {
            const { data } = await this.api.get(`dashboard/${id}`);
            return data.result;
        }
        catch (err) {
            throw wrapApiError(err, 'unable to get dashboard: ' + id);
        }
    }
    async createDashboard(dashboard) {
        try {
            const { data } = await this.api.post('dashboard/', dashboard);
            return data;
        }
        catch (err) {
            throw wrapApiError(err, 'unable to create dashboard');
        }
    }
    async updateDashboard(id, dashboard) {
        try {
            const { data } = await this.api.put(`dashboard/${id}`, dashboard);
            return data;
        }
        catch (err) {
            throw wrapApiError(err, `unable to update dashboard: ${id}`);
        }
    }
    async getCharts() {
        try {
            const { data } = await this.api.get('chart/');
            return data.result || [];
        }
        catch (err) {
            throw wrapApiError(err, 'unable to get charts');
        }
    }
    async getChart(id) {
        try {
            const { data } = await this.api.get(`chart/${id}`);
            return data.result;
        }
        catch (err) {
            throw wrapApiError(err, 'unable to get chart: ' + id);
        }
    }
    async createChart(chart) {
        try {
            const { data } = await this.api.post('chart/', chart);
            return data;
        }
        catch (err) {
            throw wrapApiError(err, 'unable to create chart');
        }
    }
    async getDatasets() {
        try {
            const { data } = await this.api.get('dataset/');
            return data.result || [];
        }
        catch (err) {
            throw wrapApiError(err, 'unable to get datasets');
        }
    }
    async createDataset(dataset) {
        try {
            const { data } = await this.api.post('dataset/', dataset);
            return data;
        }
        catch (err) {
            throw wrapApiError(err, 'unable to create dataset');
        }
    }
    async importDashboard(dashboardData) {
        try {
            const { data } = await this.api.post('dashboard/import/', dashboardData);
            return data;
        }
        catch (err) {
            throw wrapApiError(err, 'unable to import dashboard');
        }
    }
    async exportDashboard(id) {
        try {
            const { data } = await this.api.get(`dashboard/export/?q=!(${id})`);
            return data;
        }
        catch (err) {
            throw wrapApiError(err, `unable to export dashboard: ${id}`);
        }
    }
    async healthCheck() {
        try {
            const { data } = await this.api.get('/health');
            return data.status === 'ok';
        }
        catch (err) {
            return false;
        }
    }
}
exports.Superset = Superset;
