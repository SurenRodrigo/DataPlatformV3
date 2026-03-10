"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const commander_1 = require("commander");
const pino_1 = __importDefault(require("pino"));
const dashboards_1 = require("./dashboards");
const metabase_1 = require("./metabase");
const logger = (0, pino_1.default)({
    name: 'metabase-init',
    level: process.env.LOG_LEVEL || 'info',
});
async function main() {
    commander_1.program
        .requiredOption('--metabase-url <string>')
        .requiredOption('--username <string>')
        .requiredOption('--password <string>')
        .requiredOption('--database <string>')
        .addOption(new commander_1.Option('--export <dashboardId>')
        .conflicts('importOne')
        .conflicts('importNew'))
        .addOption(new commander_1.Option('--import-one <filename>')
        .conflicts('export')
        .conflicts('importNew'))
        .addOption(new commander_1.Option('--import-new').conflicts('export').conflicts('importOne'))
        .addOption(new commander_1.Option('--sync-schema'));
    commander_1.program.parse();
    const options = commander_1.program.opts();
    if (options.syncSchema) {
        const metabase = await metabase_1.Metabase.fromConfig({
            url: options.metabaseUrl,
            username: options.username,
            password: options.password,
        });
        await metabase.syncSchema(options.database);
        logger.info('Metabase sync schema triggered');
    }
    else {
        if (!options.export && !options.importOne && !options.importNew) {
            commander_1.program.help();
        }
        const dashboards = await dashboards_1.Dashboards.fromConfig({
            metabase: {
                url: options.metabaseUrl,
                username: options.username,
                password: options.password,
            },
            databaseName: options.database,
            logger,
        });
        if (options.export) {
            console.log(await dashboards.export(parseInt(options.export, 10)));
        }
        else {
            if (options.importNew) {
                await dashboards.importNew();
            }
            else {
                await dashboards.importOne(options.importOne);
            }
            logger.info('Metabase import is complete');
        }
    }
}
if (require.main === module) {
    main().catch((err) => {
        console.error(err);
        process.exit(1);
    });
}
