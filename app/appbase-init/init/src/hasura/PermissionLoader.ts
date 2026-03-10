import fs from 'fs-extra';
import path from 'path';
import pino from 'pino';
import {
  PermissionConfig,
  PermissionLoader as IPermissionLoader,
  PermissionLoaderOptions,
  ValidationResult,
  ValidationError,
  RolePermissions,
  TablePermissions,
  SelectPermission,
  InsertPermission,
  UpdatePermission,
  DeletePermission,
} from './types';

export class PermissionLoader implements IPermissionLoader {
  private readonly logger: pino.Logger;
  private readonly permissionsDir: string;
  private readonly fallbackToDefaults: boolean;
  private readonly enableValidation: boolean;

  constructor(
    resourcesDir: string,
    options: PermissionLoaderOptions = {},
    logger?: pino.Logger
  ) {
    this.logger = logger || pino({ name: 'permission-loader' });
    this.permissionsDir = path.join(resourcesDir, 'permissions');
    this.fallbackToDefaults = options.fallbackToDefaults ?? true;
    this.enableValidation = options.enableValidation ?? true;
  }

  /**
   * Load permissions from configuration files with graceful fallback
   */
  async loadPermissions(): Promise<PermissionConfig> {
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
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      this.logger.warn({ error: errorMessage }, 'Failed to load permissions from configuration files');
      
      if (this.fallbackToDefaults) {
        this.logger.info('Using default permissions as fallback');
        return this.getDefaultPermissions();
      } else {
        throw error;
      }
    }
  }

  /**
   * Load configuration files from the permissions directory
   */
  private async loadConfigurationFiles(): Promise<PermissionConfig> {
    if (!await fs.pathExists(this.permissionsDir)) {
      throw new Error(`Permissions directory does not exist: ${this.permissionsDir}`);
    }

    const files = await fs.readdir(this.permissionsDir);
    const jsonFiles = files.filter(file => file.endsWith('.json') && file !== 'permissions-schema.json');
    
    if (jsonFiles.length === 0) {
      throw new Error(`No permission configuration files found in: ${this.permissionsDir}`);
    }

    const config: PermissionConfig = { tables: {} };

    for (const file of jsonFiles) {
      const tableName = path.basename(file, '.json');
      const filePath = path.join(this.permissionsDir, file);
      
      try {
        this.logger.debug(`Loading permissions for table: ${tableName}`);
        const fileContent = await fs.readFile(filePath, 'utf8');
        const tablePermissions: TablePermissions = JSON.parse(fileContent);
        
        config.tables[tableName] = tablePermissions;
        this.logger.debug(`Successfully loaded permissions for table: ${tableName}`);
      } catch (error) {
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
  private validateConfig(config: any): ValidationResult {
    const errors: ValidationError[] = [];

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
  private validateTable(tableName: string, tableConfig: any, errors: ValidationError[]): void {
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
  private validateRole(tableName: string, roleName: string, roleConfig: any, errors: ValidationError[]): void {
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
    const rolePermissions = roleConfig as RolePermissions;

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
  private validatePermission(
    tableName: string,
    roleName: string,
    permissionType: string,
    permission: any,
    errors: ValidationError[]
  ): void {
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
        this.validateSelectPermission(path, permission as SelectPermission, errors);
        break;
      case 'insert':
        this.validateInsertPermission(path, permission as InsertPermission, errors);
        break;
      case 'update':
        this.validateUpdatePermission(path, permission as UpdatePermission, errors);
        break;
      case 'delete':
        this.validateDeletePermission(path, permission as DeletePermission, errors);
        break;
    }
  }

  /**
   * Validate select permission structure
   */
  private validateSelectPermission(path: string, permission: SelectPermission, errors: ValidationError[]): void {
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
  private validateInsertPermission(path: string, permission: InsertPermission, errors: ValidationError[]): void {
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
  private validateUpdatePermission(path: string, permission: UpdatePermission, errors: ValidationError[]): void {
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
  private validateDeletePermission(path: string, permission: DeletePermission, errors: ValidationError[]): void {
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
  private getDefaultPermissions(): PermissionConfig {
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