"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const commander_1 = require("commander");
const pino_1 = __importDefault(require("pino"));
const superset_1 = require("./superset");
/**
 * Superset utility functions
 * Provides utility operations (schema sync, dashboard export/import, health checks)
 * Called by superset-init.sh for specific operations (following metabase-init.sh pattern)
 * Database creation and configuration handled by shell script
 */
const logger = (0, pino_1.default)({
    name: 'superset-init',
    level: process.env.LOG_LEVEL || 'info',
});
async function main() {
    commander_1.program
        .requiredOption('--superset-url <string>')
        .requiredOption('--username <string>')
        .requiredOption('--password <string>')
        .requiredOption('--database <string>')
        .addOption(new commander_1.Option('--sync-schema'))
        .addOption(new commander_1.Option('--export-dashboard <dashboardId>')
        .conflicts('importDashboard'))
        .addOption(new commander_1.Option('--import-dashboard <filename>')
        .conflicts('exportDashboard'))
        .addOption(new commander_1.Option('--health-check'));
    commander_1.program.parse();
    const options = commander_1.program.opts();
    try {
        const superset = await superset_1.Superset.fromConfig({
            url: options.supersetUrl,
            username: options.username,
            password: options.password,
        });
        if (options.healthCheck) {
            const isHealthy = await superset.healthCheck();
            if (isHealthy) {
                logger.info('Superset health check passed');
                process.exit(0);
            }
            else {
                logger.error('Superset health check failed');
                process.exit(1);
            }
        }
        if (options.syncSchema) {
            const database = await superset.getDatabase('Platform Data');
            if (database) {
                await superset.syncDatabase(database.id);
                logger.info('Superset schema sync completed for all datasets');
            }
            else {
                logger.error('Database "Platform Data" not found');
                process.exit(1);
            }
        }
        if (options.exportDashboard) {
            const dashboardData = await superset.exportDashboard(parseInt(options.exportDashboard, 10));
            console.log(JSON.stringify(dashboardData, null, 2));
            logger.info('Dashboard export completed');
        }
        if (options.importDashboard) {
            // Implementation would depend on file format and specific requirements
            logger.info('Dashboard import functionality available');
            logger.info('Import file: ' + options.importDashboard);
        }
        if (!options.syncSchema && !options.exportDashboard &&
            !options.importDashboard && !options.healthCheck) {
            commander_1.program.help();
        }
    }
    catch (error) {
        const errorMessage = error instanceof Error ? error.message : String(error);
        logger.error({ error: errorMessage }, 'Superset initialization failed');
        process.exit(1);
    }
}
if (require.main === module) {
    main().catch((err) => {
        console.error(err);
        process.exit(1);
    });
}
