"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.loadDashboards = loadDashboards;
exports.loadDashboard = loadDashboard;
const fs_extra_1 = __importDefault(require("fs-extra"));
const path_1 = __importDefault(require("path"));
const config_1 = require("../config");
async function loadDashboards() {
    const dir = path_1.default.join(config_1.BASE_RESOURCES_DIR, 'metabase', 'dashboards');
    const dirents = await fs_extra_1.default.readdir(dir, { withFileTypes: true });
    const promises = dirents
        .filter((dirent) => dirent.isFile() && !dirent.name.startsWith('.'))
        .map(async (dirent) => {
        const dashboardPath = path_1.default.join(dir, dirent.name);
        return {
            name: path_1.default.parse(dashboardPath).name,
            template: await fs_extra_1.default.readFile(dashboardPath, 'utf-8'),
        };
    });
    return await Promise.all(promises);
}
async function loadDashboard(name) {
    const dashboardPath = path_1.default.join(config_1.BASE_RESOURCES_DIR, 'metabase', 'dashboards', name);
    const template = await fs_extra_1.default.readFile(dashboardPath, 'utf-8');
    return { name, template };
}
