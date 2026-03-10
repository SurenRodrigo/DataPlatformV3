#!/usr/bin/env node

/**
 * Parse service.yaml and return service state
 * Usage: node parse-service-yaml.js <service-path>
 * Example: node parse-service-yaml.js platform.hasura
 * Returns: "true", "false", or "local"/"external" for db
 */

const fs = require('fs');
const path = require('path');
const yaml = require('js-yaml');

const SERVICE_YAML_PATH = '/app/service.yaml';

function getServiceState(servicePath) {
  try {
    // Check if service.yaml exists
    if (!fs.existsSync(SERVICE_YAML_PATH)) {
      console.error(`Error: ${SERVICE_YAML_PATH} not found`);
      process.exit(1);
    }

    // Read and parse YAML file
    const fileContents = fs.readFileSync(SERVICE_YAML_PATH, 'utf8');
    const serviceConfig = yaml.load(fileContents);

    if (!serviceConfig) {
      console.error('Error: service.yaml is empty or invalid');
      process.exit(1);
    }

    // Navigate through the path (e.g., "platform.hasura" -> serviceConfig.platform.hasura)
    const pathParts = servicePath.split('.');
    let value = serviceConfig;

    for (const part of pathParts) {
      if (value === null || value === undefined) {
        return 'false';
      }
      value = value[part];
    }

    // Return the value as string
    if (value === null || value === undefined) {
      return 'false';
    }

    // Convert boolean to string
    if (typeof value === 'boolean') {
      return value ? 'true' : 'false';
    }

    // Return string value (for db: "local" or "external")
    return String(value);
  } catch (error) {
    console.error(`Error parsing service.yaml: ${error.message}`);
    process.exit(1);
  }
}

// Get service path from command line arguments
const servicePath = process.argv[2];

if (!servicePath) {
  console.error('Usage: node parse-service-yaml.js <service-path>');
  console.error('Example: node parse-service-yaml.js platform.hasura');
  process.exit(1);
}

// Get and output service state
const state = getServiceState(servicePath);
console.log(state);
