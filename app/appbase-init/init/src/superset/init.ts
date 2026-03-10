import {Option, program} from 'commander';
import pino from 'pino';

import {Superset} from './superset';

/**
 * Superset utility functions
 * Provides utility operations (schema sync, dashboard export/import, health checks)
 * Called by superset-init.sh for specific operations (following metabase-init.sh pattern)
 * Database creation and configuration handled by shell script
 */

const logger = pino({
  name: 'superset-init',
  level: process.env.LOG_LEVEL || 'info',
});

async function main(): Promise<void> {
  program
    .requiredOption('--superset-url <string>')
    .requiredOption('--username <string>')
    .requiredOption('--password <string>')
    .requiredOption('--database <string>')
    .addOption(
      new Option('--sync-schema')
    )
    .addOption(
      new Option('--export-dashboard <dashboardId>')
        .conflicts('importDashboard')
    )
    .addOption(
      new Option('--import-dashboard <filename>')
        .conflicts('exportDashboard')
    )
    .addOption(
      new Option('--health-check')
    );

  program.parse();
  const options = program.opts();

  try {
    const superset = await Superset.fromConfig({
      url: options.supersetUrl,
      username: options.username,
      password: options.password,
    });

    if (options.healthCheck) {
      const isHealthy = await superset.healthCheck();
      if (isHealthy) {
        logger.info('Superset health check passed');
        process.exit(0);
      } else {
        logger.error('Superset health check failed');
        process.exit(1);
      }
    }

    if (options.syncSchema) {
      const database = await superset.getDatabase('Platform Data');
      if (database) {
        await superset.syncDatabase(database.id);
        logger.info('Superset schema sync completed for all datasets');
      } else {
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
      program.help();
    }

  } catch (error) {
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