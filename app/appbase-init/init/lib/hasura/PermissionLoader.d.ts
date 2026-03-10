import pino from 'pino';
import { PermissionConfig, PermissionLoader as IPermissionLoader, PermissionLoaderOptions } from './types';
export declare class PermissionLoader implements IPermissionLoader {
    private readonly logger;
    private readonly permissionsDir;
    private readonly fallbackToDefaults;
    private readonly enableValidation;
    constructor(resourcesDir: string, options?: PermissionLoaderOptions, logger?: pino.Logger);
    /**
     * Load permissions from configuration files with graceful fallback
     */
    loadPermissions(): Promise<PermissionConfig>;
    /**
     * Load configuration files from the permissions directory
     */
    private loadConfigurationFiles;
    /**
     * Validate permission configuration structure and content
     */
    private validateConfig;
    /**
     * Validate individual table configuration
     */
    private validateTable;
    /**
     * Validate individual role configuration
     */
    private validateRole;
    /**
     * Validate individual permission configuration
     */
    private validatePermission;
    /**
     * Validate select permission structure
     */
    private validateSelectPermission;
    /**
     * Validate insert permission structure
     */
    private validateInsertPermission;
    /**
     * Validate update permission structure
     */
    private validateUpdatePermission;
    /**
     * Validate delete permission structure
     */
    private validateDeletePermission;
    /**
     * Get default permissions (existing hardcoded permissions as fallback)
     */
    private getDefaultPermissions;
}
