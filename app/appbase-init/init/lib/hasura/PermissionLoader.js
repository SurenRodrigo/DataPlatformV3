"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.PermissionLoader = void 0;
const fs_extra_1 = __importDefault(require("fs-extra"));
const path_1 = __importDefault(require("path"));
const pino_1 = __importDefault(require("pino"));
class PermissionLoader {
    constructor(resourcesDir, options = {}, logger) {
        var _a, _b;
        this.logger = logger || (0, pino_1.default)({ name: 'permission-loader' });
        this.permissionsDir = path_1.default.join(resourcesDir, 'permissions');
        this.fallbackToDefaults = (_a = options.fallbackToDefaults) !== null && _a !== void 0 ? _a : true;
        this.enableValidation = (_b = options.enableValidation) !== null && _b !== void 0 ? _b : true;
    }
    /**
     * Load permissions from configuration files with graceful fallback
     */
    async loadPermissions() {
        try {
            this.logger.info('Loading permissions from configuration files');
            const config = await this.loadConfigurationFiles();
            if (this.enableValidation) {
                const validationResult = this.validateConfig(config);
                if (!validationResult.valid) {
                    throw new Error(`Configuration validation failed: ${validationResult.errors.map(e => e.message).join(', ')}`);
                }
            }
            this.logger.info('Permissions loaded successfully from configuration files');
            return config;
        }
        catch (error) {
            const errorMessage = error instanceof Error ? error.message : String(error);
            this.logger.warn({ error: errorMessage }, 'Failed to load permissions from configuration files');
            if (this.fallbackToDefaults) {
                this.logger.info('Using default permissions as fallback');
                return this.getDefaultPermissions();
            }
            else {
                throw error;
            }
        }
    }
    /**
     * Load configuration files from the permissions directory
     */
    async loadConfigurationFiles() {
        if (!await fs_extra_1.default.pathExists(this.permissionsDir)) {
            throw new Error(`Permissions directory does not exist: ${this.permissionsDir}`);
        }
        const files = await fs_extra_1.default.readdir(this.permissionsDir);
        const jsonFiles = files.filter(file => file.endsWith('.json') && file !== 'permissions-schema.json');
        if (jsonFiles.length === 0) {
            throw new Error(`No permission configuration files found in: ${this.permissionsDir}`);
        }
        const config = { tables: {} };
        for (const file of jsonFiles) {
            const tableName = path_1.default.basename(file, '.json');
            const filePath = path_1.default.join(this.permissionsDir, file);
            try {
                this.logger.debug(`Loading permissions for table: ${tableName}`);
                const fileContent = await fs_extra_1.default.readFile(filePath, 'utf8');
                const tablePermissions = JSON.parse(fileContent);
                config.tables[tableName] = tablePermissions;
                this.logger.debug(`Successfully loaded permissions for table: ${tableName}`);
            }
            catch (error) {
                const errorMessage = error instanceof Error ? error.message : String(error);
                this.logger.error({ error: errorMessage, file: filePath }, `Failed to load permissions for table ${tableName}`);
                throw new Error(`Failed to parse permission file ${file}: ${errorMessage}`);
            }
        }
        this.logger.info(`Loaded permissions for ${Object.keys(config.tables).length} tables`);
        return config;
    }
    /**
     * Validate permission configuration structure and content
     */
    validateConfig(config) {
        const errors = [];
        // Structure validation
        if (!config || typeof config !== 'object') {
            errors.push({
                path: 'root',
                message: 'Configuration must be an object',
                code: 'INVALID_TYPE'
            });
            return { valid: false, errors };
        }
        if (!config.tables || typeof config.tables !== 'object') {
            errors.push({
                path: 'tables',
                message: 'Configuration must have a tables object',
                code: 'MISSING_TABLES'
            });
            return { valid: false, errors };
        }
        // Validate each table
        for (const [tableName, tableConfig] of Object.entries(config.tables)) {
            this.validateTable(tableName, tableConfig, errors);
        }
        return { valid: errors.length === 0, errors };
    }
    /**
     * Validate individual table configuration
     */
    validateTable(tableName, tableConfig, errors) {
        if (!tableConfig || typeof tableConfig !== 'object') {
            errors.push({
                path: `tables.${tableName}`,
                message: `Table configuration must be an object`,
                code: 'INVALID_TABLE_CONFIG'
            });
            return;
        }
        // Validate each role in the table
        for (const [roleName, roleConfig] of Object.entries(tableConfig)) {
            this.validateRole(tableName, roleName, roleConfig, errors);
        }
    }
    /**
     * Validate individual role configuration
     */
    validateRole(tableName, roleName, roleConfig, errors) {
        const path = `tables.${tableName}.${roleName}`;
        if (!roleConfig || typeof roleConfig !== 'object') {
            errors.push({
                path,
                message: `Role configuration must be an object`,
                code: 'INVALID_ROLE_CONFIG'
            });
            return;
        }
        const validPermissionTypes = ['select', 'insert', 'update', 'delete'];
        const rolePermissions = roleConfig;
        // Validate each permission type
        for (const [permissionType, permission] of Object.entries(rolePermissions)) {
            if (!validPermissionTypes.includes(permissionType)) {
                errors.push({
                    path: `${path}.${permissionType}`,
                    message: `Invalid permission type. Must be one of: ${validPermissionTypes.join(', ')}`,
                    code: 'INVALID_PERMISSION_TYPE'
                });
                continue;
            }
            this.validatePermission(tableName, roleName, permissionType, permission, errors);
        }
    }
    /**
     * Validate individual permission configuration
     */
    validatePermission(tableName, roleName, permissionType, permission, errors) {
        const path = `tables.${tableName}.${roleName}.${permissionType}`;
        if (!permission || typeof permission !== 'object') {
            errors.push({
                path,
                message: `Permission must be an object`,
                code: 'INVALID_PERMISSION'
            });
            return;
        }
        switch (permissionType) {
            case 'select':
                this.validateSelectPermission(path, permission, errors);
                break;
            case 'insert':
                this.validateInsertPermission(path, permission, errors);
                break;
            case 'update':
                this.validateUpdatePermission(path, permission, errors);
                break;
            case 'delete':
                this.validateDeletePermission(path, permission, errors);
                break;
        }
    }
    /**
     * Validate select permission structure
     */
    validateSelectPermission(path, permission, errors) {
        if (!permission.columns) {
            errors.push({
                path: `${path}.columns`,
                message: 'Select permission must have columns',
                code: 'MISSING_COLUMNS'
            });
        }
        if (permission.filter === undefined || permission.filter === null) {
            errors.push({
                path: `${path}.filter`,
                message: 'Select permission must have filter (can be empty object)',
                code: 'MISSING_FILTER'
            });
        }
    }
    /**
     * Validate insert permission structure
     */
    validateInsertPermission(path, permission, errors) {
        if (!permission.columns) {
            errors.push({
                path: `${path}.columns`,
                message: 'Insert permission must have columns',
                code: 'MISSING_COLUMNS'
            });
        }
        if (permission.check === undefined || permission.check === null) {
            errors.push({
                path: `${path}.check`,
                message: 'Insert permission must have check (can be empty object)',
                code: 'MISSING_CHECK'
            });
        }
    }
    /**
     * Validate update permission structure
     */
    validateUpdatePermission(path, permission, errors) {
        if (!permission.columns) {
            errors.push({
                path: `${path}.columns`,
                message: 'Update permission must have columns',
                code: 'MISSING_COLUMNS'
            });
        }
        if (permission.filter === undefined || permission.filter === null) {
            errors.push({
                path: `${path}.filter`,
                message: 'Update permission must have filter (can be empty object)',
                code: 'MISSING_FILTER'
            });
        }
    }
    /**
     * Validate delete permission structure
     */
    validateDeletePermission(path, permission, errors) {
        if (permission.filter === undefined || permission.filter === null) {
            errors.push({
                path: `${path}.filter`,
                message: 'Delete permission must have filter (can be empty object)',
                code: 'MISSING_FILTER'
            });
        }
    }
    /**
     * Get default permissions (existing hardcoded permissions as fallback)
     */
    getDefaultPermissions() {
        this.logger.info('Providing default hardcoded permissions');
        return {
            tables: {
                users: {
                    AppBaseAdmin: {
                        select: { columns: '*', filter: {} },
                        insert: { columns: '*', check: {} },
                        update: { columns: '*', filter: {} },
                        delete: { filter: {} }
                    },
                    User: {
                        select: { columns: '*', filter: {} }
                    }
                },
                user_login_details: {
                    AppBaseAdmin: {
                        select: { columns: '*', filter: {} },
                        insert: { columns: '*', check: {} },
                        update: { columns: '*', filter: {} },
                        delete: { filter: {} }
                    },
                    User: {
                        select: {
                            columns: '*',
                            filter: { user_id: { _eq: 'X-Hasura-User-Id' } }
                        }
                    }
                }
            }
        };
    }
}
exports.PermissionLoader = PermissionLoader;
