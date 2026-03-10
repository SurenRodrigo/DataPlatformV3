"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.BASE_RESOURCES_DIR = exports.PACKAGE_ROOT = void 0;
const path_1 = __importDefault(require("path"));
exports.PACKAGE_ROOT = path_1.default.join(__dirname, '..');
exports.BASE_RESOURCES_DIR = path_1.default.join(exports.PACKAGE_ROOT, 'resources');
