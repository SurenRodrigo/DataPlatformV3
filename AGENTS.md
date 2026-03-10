# AGENTS.md

A comprehensive guide for AI coding agents working on the 99x Data Platform.

## Project Overview

The 99x Data Platform is a **two-phase, containerized microservices architecture** with clear separation between platform infrastructure and application services. The system follows a modern containerization approach using Docker Compose with profile-based service deployment.

**Key Architecture Pattern**: Platform layer (`platform/`) manages core infrastructure services, while application layer (`app/`) manages business logic and data orchestration.

## Project Structure

```
data-platform-99x-v2/
├── service.yaml               # Service configuration (SINGLE SOURCE OF TRUTH)
│                              # Controls which services are enabled/disabled
│                              # Read by: platform/start.sh, app/start.sh, 
│                              #          postgres-entrypoint.sh, appbase-init scripts
├── platform/                  # Platform infrastructure services
│   ├── docker-compose.yaml    # Platform services orchestration
│   ├── docker-compose.no-db.yaml # Override file for external database mode
│   ├── start.sh               # Platform startup script (reads service.yaml)
│   ├── stop.sh                # Platform shutdown script (reads service.yaml)
│   ├── nginx.conf             # Nginx reverse proxy configuration (dev)
│   ├── nginx.dev.conf         # Development nginx configuration
│   ├── nginx.prod.conf        # Production nginx configuration
│   ├── nginx.htpasswd         # Basic auth credentials for Dagster
│   ├── dagster.yaml           # Dagster platform configuration
│   ├── workspace.yaml         # Dagster workspace configuration
│   ├── postgres-entrypoint.sh # Custom PostgreSQL initialization (reads service.yaml)
│   ├── superset_config.py     # Superset configuration
│   ├── superset_entrypoint.sh # Superset initialization script
│   ├── Dockerfile.dagster     # Custom Dagster image (multi-stage, hardened)
│   ├── Dockerfile.nginx       # Custom Nginx image
│   ├── Dockerfile.superset    # Custom Superset image (multi-stage, hardened)
│   ├── Dockerfile.metabase    # Custom Metabase image (multi-stage, hardened)
│   ├── requirements.txt       # Python dependencies
│   ├── certs/                 # SSL certificates directory
│   ├── superset_assets/       # Superset static assets
│   ├── .env                   # Platform environment variables
│   ├── _env_sample            # Platform environment variable template
│   └── _env_live              # Platform environment variables (production)
├── app/                       # Application services and logic
│   ├── docker-compose.yaml    # Application services orchestration
│   ├── start.sh               # Application startup script (reads service.yaml)
│   ├── stop.sh                # Application shutdown script (reads service.yaml)
│   ├── Dockerfile.appbase-init # AppBase init service image
│   ├── Dockerfile.data-platform-service # Data platform service image
│   ├── client/                # Next.js frontend client app
│   │   ├── Dockerfile         # Client application image
│   │   ├── package.json       # Node.js dependencies
│   │   ├── next.config.ts     # Next.js configuration
│   │   ├── src/               # Source code
│   │   └── documentation/     # Client architecture docs
│   ├── data-platform-service/ # Data platform service (all data orchestration)
│   │   ├── data-manager/      # Data orchestration core
│   │   │   ├── dagster.yaml   # Dagster app configuration
│   │   │   ├── entrypoint.sh  # Data platform service startup script
│   │   │   ├── requirements.txt # Python dependencies
│   │   │   ├── pyairbyte/     # PyAirbyte integration and utilities
│   │   │   ├── external-connectors/ # External connector configs (YAML)
│   │   │   ├── resources/     # Dagster resources
│   │   │   └── scripts/       # Utility scripts
│   │   ├── dagster_code/      # Dagster code locations
│   │   │   └── bridgestone_data_sync/ # Bridgestone data sync pipeline (port 4273)
│   │   └── dbt_models/        # DBT transformation models
│   │       ├── dbt_project.yml # DBT project configuration
│   │       ├── models/        # DBT models
│   │       ├── profiles.yml   # DBT profiles
│   │       └── dbt_packages/  # DBT packages
│   ├── appbase-init/          # AppBase initialization service
│   │   ├── appbase-schemas/   # Database migration files
│   │   │   ├── V1__create_init_tables.sql
│   │   │   ├── V2__create_init_indexes.sql
│   │   │   ├── V3__create_init_foreign_keys.sql
│   │   │   ├── V4__sample_init_data.sql
│   │   │   ├── V5__create_invoice_table.sql
│   │   │   └── V6__create_credit_data_table.sql
│   │   └── init/              # Application initialization service
│   │       ├── package.json   # Node.js dependencies
│   │       ├── src/           # TypeScript source code
│   │       ├── scripts/       # Initialization scripts
│   │       │   ├── entrypoint.sh # Main entrypoint (reads service.yaml)
│   │       │   ├── db-init.sh    # DB initialization (reads service.yaml)
│   │       │   └── parse-service-yaml.js # YAML parser utility
│   │       └── resources/     # Initialization resources
│   ├── .env                   # Application environment variables
│   ├── _env_sample            # Application environment variable template
│   └── _env_live              # Application environment variables (production)
└── docs/                      # Architecture documentation
```

## Core Services Architecture

### Platform Services
- **Database**: PostgreSQL 15-alpine with multiple schemas
- **GraphQL Engine**: Hasura for auto-generated GraphQL APIs
- **BI Tools**: Metabase/Superset (profile-based)
- **Web Database Manager**: CloudBeaver Community (profile-based, optional)
- **Data Orchestration**: Dagster webserver + daemon
- **Reverse Proxy**: Nginx with comprehensive routing

### Application Services
- **AppBase Init**: Node.js/TypeScript service (Node.js 24.11.1 LTS) for application initialization and database setup using Flyway
- **Data Platform Service**: Python service (Python 3.11) for Dagster gRPC servers and PyAirbyte integration
  - Contains: `data-manager/`, `dagster_code/`, `dbt_models/` (all organized under `data-platform-service/`)
- **Client**: Next.js frontend application with TypeScript (not deployed as Docker service, runs separately)
- **Dagster Code Locations**: Data pipeline definitions and orchestration (1 active location: bridgestone_data_sync)
- **DBT Models**: Data transformation layer with staging and marts
- **PyAirbyte Integration**: External data connector management with cache database support
- **Database Migrations**: Flyway-style schema management (V1-V6 migrations)

## Setup Commands

### Platform Setup
```bash
cd platform
cp _env_sample .env  # Configure platform environment
./start.sh          # Start platform services
```

### Application Setup
```bash
cd app
cp _env_sample .env  # Configure application environment
./start.sh          # Start application services
```

### Frontend Development
```bash
cd app/client
npm install
npm run dev
```

## Docker Compose Profiles and Service Configuration

The platform uses Docker Compose profiles for optional service deployment. **Service configuration is managed via `service.yaml` at the project root, which is the SINGLE SOURCE OF TRUTH for all service enablement decisions.**

### Platform Services (configured in `service.yaml`):
- **local-db**: Local PostgreSQL database container (or use external database with `db: external`)
- **hasura**: GraphQL engine (can be toggled)
- **metabase**: Metabase BI tool (can be toggled)
- **superset**: Superset BI tool (can be toggled)
- **cloudbeaver**: CloudBeaver Community web database manager (can be toggled)
- **dagster**: Dagster orchestration services (webserver + daemon, toggles both together)
- **nginx**: Nginx reverse proxy (can be toggled)

### App Services (configured in `service.yaml`):
- **appbase-init**: Mandatory service (always runs, not toggleable) - initializes platform services based on service.yaml
- **data-platform-service**: Data orchestration service with Dagster gRPC servers (can be toggled)

**Service Configuration**: All service toggles are configured in `service.yaml` at the project root. Edit this file to enable/disable services:

```yaml
platform:
  db: local  # "local" or "external"
  hasura: true
  metabase: false
  superset: true
  cloudbeaver: false
  dagster: true
  nginx: false

app:
  # appbase-init is mandatory and always runs (not toggleable)
  data-platform-service: true  # Data orchestration service (Dagster gRPC servers)
```

### Profile Usage Examples
```bash
# Start platform with services configured in service.yaml
cd platform && ./start.sh

# Rebuild without cache
cd platform && ./start.sh --no-cache

# Start app services (appbase-init always runs, data-platform-service based on service.yaml)
cd app && ./start.sh
```

**Important Notes**:
- Service configuration is read from `service.yaml` by both platform and app startup scripts
- `appbase-init` reads from `service.yaml` to conditionally initialize platform services (Hasura, Metabase, Superset)
- The database container (`postgres-entrypoint.sh`) reads from `service.yaml` to create service databases
- **Environment variables are NO LONGER USED for feature toggling** - they are only used for connection details (URLs, credentials, etc.)

## Service Dependencies

```
Platform: db → hasura → [metabase|superset] → [cloudbeaver] → [dagster-webserver|dagster-daemon] → nginx
App: appbase-init → data-platform-service
```

### Detailed Service Flow
1. **Platform Layer**: Database → Hasura → BI Tools → CloudBeaver (optional) → Dagster → Nginx
2. **Application Layer**: AppBase Init → Data Platform Service
3. **Cross-Layer**: Data Platform Service connects to Dagster via gRPC (port 4273)
4. **Client**: Next.js frontend runs separately (not as Docker service), connects to Hasura GraphQL API

## Network Architecture

- **External Network**: `app-base-network` for cross-compose communication
- **Internal Network**: `appbase_intternal` for platform services
- **Shared Volumes**: `dagster_shared_storage` between platform and app
- **gRPC Communication**: Data Platform Service runs gRPC server:
  - Port 4273: bridgestone_data_sync code location
- **Subdomain Routing**: Nginx routes traffic based on subdomains (hasura.localhost, dagster.localhost, superset.localhost, cloudbeaver.localhost)

## Environment Configuration

### Platform Environment Variables
- **Database**: `DATABASE_*`, `APPBASE_CONFIG_DB_*` (PostgreSQL configuration)
- **Hasura**: `HASURA_*` (GraphQL engine settings - connection details only)
- **BI Tools**: `METABASE_*`, `SUPERSET_*` (Business intelligence configuration - connection details only)
- **Dagster**: `DAGSTER_*` (Data orchestration settings - connection details only)
- **Nginx**: `NGINX_PORT`, `ENVIRONMENT` (Reverse proxy configuration: dev/prod)
- **Security**: `APP_BASE_SECRET` (Application security)

**Service Feature Toggles**: 
- **IMPORTANT**: Service enable/disable is configured in `service.yaml` at project root, NOT via environment variables
- Environment variables (`WITH_BI`, `WITH_BI_TOOL`, `WITH_ALL_BI`, `WITH_DAGSTER`) are **NO LONGER USED** for feature toggling
- Environment variables are only used for connection details (URLs, credentials, database names, ports)
- Both platform and app services read from `service.yaml` for service enablement decisions
- The database container (`postgres-entrypoint.sh`) reads from `service.yaml` to determine which service databases to create

### Application Environment Variables
- **Database**: `APPBASE_DB_*`, `APPBASE_CONFIG_DB_*` (Application database connections)
- **Hasura**: `HASURA_URL`, `HASURA_GRAPHQL_ADMIN_SECRET` (GraphQL integration - connection details only)
- **BI Tools**: `METABASE_*`, `SUPERSET_*` (BI tool connection URLs and credentials - connection details only)
- **Dagster**: `DAGSTER_*`, `DAGSTER_HOME`, `DAGSTER_GRPC_*` (Data orchestration - connection details only)
- **PyAirbyte**: `PYAIRBYTE_*` (External connector configuration)
- **Init Service**: `APPBASE_INIT_*`, `RUN_DB_MIGRATIONS` (Initialization settings)
- **Integration APIs**: External API configurations as needed for specific data pipelines

**Service Feature Toggles**: 
- **IMPORTANT**: Service enable/disable is configured in `service.yaml` at project root, NOT via environment variables
- The `appbase-init` service reads service states from `service.yaml` (mounted at `/app/service.yaml`) to conditionally initialize Hasura, Metabase, and Superset
- The `appbase-init/init/scripts/entrypoint.sh` validates that required env vars are set when services are enabled in `service.yaml`
- The `appbase-init/init/scripts/db-init.sh` verifies service databases exist only for services enabled in `service.yaml`
- The `data-platform-service` is toggleable via `app.data-platform-service` in `service.yaml`
- **No environment variables are used for service toggling** - they are only used for connection details

## Development Workflow

### 1. Start Platform Services
```bash
cd platform
./start.sh [--no-cache]
```

**Flags:**
- `--no-cache`: Build images without cache

**Note**: Service enablement (hasura, metabase, superset, cloudbeaver, dagster, nginx, db mode) is controlled via `service.yaml` at project root, not via CLI flags. The `--with-nginx` and `--no-local-db` flags have been removed in favor of `service.yaml` configuration.

### 2. Start Application Services
```bash
cd app
./start.sh [--no-cache]
```

### 3. Monitor Services
```bash
# Platform services
docker compose -f platform/docker-compose.yaml ps

# Application services
docker compose -f app/docker-compose.yaml ps

# Service logs
docker compose -f platform/docker-compose.yaml logs -f
docker compose -f app/docker-compose.yaml logs -f
```

## Code Style and Conventions

### Docker Compose
- Use version "3.8" specification
- Implement health checks for all services
- Use external networks for cross-compose communication
- Follow profile-based service management

### Environment Variables
- Use descriptive names with `APPBASE_` prefix for platform
- Use `APPBASE_` prefix for application configuration
- Document all required variables in `_env_sample` files

### Service Configuration
- Implement comprehensive health checks
- Use proper logging configuration
- Follow container best practices (non-root users, proper volumes)

## Testing and Validation

### Service Health Checks
- **Database**: PostgreSQL health check via `pg_isready`
- **Hasura**: GraphQL endpoint availability
- **Dagster**: Webserver API availability (port 3030)
- **Data Platform Service**: gRPC server health check (port 4273 for bridgestone_data_sync code location)
- **BI Tools**: Web interface availability
- **CloudBeaver**: Web interface availability (port 8978)
- **Nginx**: HTTP endpoint availability with health endpoint
- **AppBase Init**: One-time initialization service - verifies completion via exit code (0 = success), not running status

### Validation Commands
```bash
# Check platform services
cd platform && docker compose ps

# Check application services
cd app && docker compose ps

# Test service connectivity
curl http://localhost:8081/healthz  # Hasura
curl http://localhost:3030/health   # Dagster
curl http://localhost/health        # Nginx
curl http://localhost:4273/health   # Bridgestone data sync gRPC server
curl http://localhost:8978/         # CloudBeaver (when enabled)
```

## Common Issues and Solutions

### Service Startup Order
- Platform services must start before application services
- Database must be healthy before other services
- Database container reads from `service.yaml` to create service databases (Metabase, Superset, Dagster)
- AppBase Init must complete before Data Platform Service starts (if enabled)
- AppBase Init reads from `service.yaml` to conditionally initialize platform services
- Data Platform Service gRPC servers must be running before Dagster webserver can discover them (if enabled)
- Use health checks to manage dependencies
- Client runs separately (not as Docker service) and connects to Hasura GraphQL API
- **AppBase Init is a one-time initialization service** - it runs, completes initialization, and exits (does not stay running)

### Network Issues
- Ensure `app-base-network` exists before starting services
- Check external network configuration in docker-compose files
- Verify service communication across compose boundaries

### Volume Mounts
- Dagster shared storage must be accessible to both platform and app
- Use proper volume permissions and ownership
- Check volume mount paths in docker-compose files
- Ensure PyAirbyte cache database is properly configured
- Verify DBT models and profiles are mounted correctly
- Dagster code locations mounted for hot reloading: `./data-platform-service/dagster_code:/app/dagster_code:delegated`
- All data platform components organized under `data-platform-service/` directory

## Security Considerations

- **Hardened Container Images**: All platform services use security-hardened container images
  - **Hasura**: Official image upgraded to v2.48.6 (94.4% vulnerability reduction)
  - **Metabase**: Custom multi-stage Dockerfile with fixed JAR dependencies (0 Critical, 0 High vulnerabilities)
  - **Superset**: Custom multi-stage build from source on Ubuntu 24.04 LTS (0 Critical, 0 High vulnerabilities, 68% size reduction)
  - **Dagster**: Custom multi-stage Dockerfile with minimal attack surface (0 Critical, 0 High vulnerabilities)
- Basic authentication implemented for Dagster access via nginx.htpasswd
- Environment variables for sensitive configuration
- Network isolation between platform and application layers
- Proper container security practices (non-root users, minimal images, multi-stage builds)
- Subdomain-based routing for service isolation
- SSL/TLS support via certs directory
- All images built with `--platform linux/amd64` for production Linux compatibility

## Deployment Notes

- **Two-Phase Deployment**: Platform first, then application
- **Profile-Based**: Services can be enabled/disabled via profiles
- **External Networks**: Cross-compose communication via external networks
- **Shared Storage**: Dagster configuration shared between platform and app
- **External Database Mode**: Use `--no-local-db` flag with `docker-compose.no-db.yaml` override file
- **gRPC Server**: Data Platform Service runs 1 code location on port 4273 (bridgestone_data_sync)
- **Configuration-Driven**: Code locations defined in `app/data-platform-service/data-manager/resources/dagster/code-locations.json`
- **Organized Structure**: All data platform components (data-manager, dagster_code, dbt_models) under `data-platform-service/`

## Key Files for Understanding

- `service.yaml` - **Service configuration file (SINGLE SOURCE OF TRUTH)** at project root. Controls which services are enabled/disabled for both platform and app layers. 
  - Read by: `platform/start.sh`, `platform/stop.sh`, `platform/postgres-entrypoint.sh`, `app/start.sh`, `app/stop.sh`, `app/appbase-init/init/scripts/entrypoint.sh`, `app/appbase-init/init/scripts/db-init.sh`
  - Mounted to: Database container (`/app/service.yaml`), AppBase Init container (`/app/service.yaml`)
  - **No environment variables are used for service toggling** - all enablement decisions come from this file
- `platform/docker-compose.yaml` - Platform service orchestration
- `platform/docker-compose.no-db.yaml` - Override file for external database mode
- `app/docker-compose.yaml` - Application service orchestration
- `platform/start.sh` - Platform startup orchestration (reads service configuration from service.yaml)
- `app/start.sh` - Application startup orchestration (validates service.yaml exists)
- `platform/nginx.conf` - Reverse proxy configuration (dev)
- `platform/nginx.dev.conf` - Development nginx configuration
- `platform/nginx.prod.conf` - Production nginx configuration
- `platform/dagster.yaml` - Dagster platform configuration
- `platform/workspace.yaml` - Dagster workspace configuration (defines gRPC code locations)
- `app/data-platform-service/data-manager/dagster.yaml` - Dagster app configuration
- `app/data-platform-service/data-manager/resources/dagster/code-locations.json` - Code location definitions (bridgestone_data_sync)
- `app/data-platform-service/data-manager/scripts/dagster-init.sh` - Multi-gRPC server initialization script
- `app/appbase-init/appbase-schemas/` - Database migration files (V1-V6)
- `app/appbase-init/init/` - Application initialization service (TypeScript source, scripts, resources)
- `app/appbase-init/init/scripts/entrypoint.sh` - Application initialization entrypoint (reads service.yaml for conditional initialization, validates env vars when services are enabled)
- `app/appbase-init/init/scripts/db-init.sh` - Database initialization script (verifies service databases exist only for services enabled in service.yaml)
- `app/appbase-init/init/scripts/parse-service-yaml.js` - Node.js utility to parse service.yaml (used by entrypoint.sh and db-init.sh)
- `platform/postgres-entrypoint.sh` - Database container initialization script (reads service.yaml to create service databases)
- `app/data-platform-service/dbt_models/` - DBT transformation models
- `app/data-platform-service/data-manager/pyairbyte/` - External connector integration and utilities
- `app/Dockerfile.appbase-init` - AppBase Init image (multi-stage: Node.js 24.11.1 LTS + Flyway)
- `app/Dockerfile.data-platform-service` - Data Platform Service image (multi-stage: Python 3.11 + Microsoft ODBC drivers)
- `platform/Dockerfile.metabase` - Custom hardened Metabase image (multi-stage build)
- `platform/Dockerfile.superset` - Custom hardened Superset image (multi-stage build from source)
- `platform/Dockerfile.dagster` - Custom hardened Dagster image (multi-stage build)
- `platform/_env_sample` - Platform environment variable template with hardened image versions

## Quick Reference Commands

```bash
# Configure services in service.yaml first
# Edit service.yaml at project root to enable/disable services

# Start entire platform (services enabled in service.yaml)
cd platform && ./start.sh

# Start application after platform (appbase-init always runs, data-platform-service based on service.yaml)
cd app && ./start.sh

# Stop all services
cd platform && ./stop.sh
cd app && ./stop.sh

# View service logs
docker compose -f platform/docker-compose.yaml logs -f
docker compose -f app/docker-compose.yaml logs -f

# Check appbase-init completion (one-time service)
docker compose -f app/docker-compose.yaml logs appbase-init
docker inspect appbase-init --format='{{.State.ExitCode}}'  # 0 = success

# Rebuild services
cd platform && ./start.sh --no-cache
cd app && ./start.sh --no-cache
```

## Architecture Principles

1. **Separation of Concerns**: Platform vs. Application layers
2. **Profile-Based Deployment**: Optional services via Docker Compose profiles
3. **Service Configuration as Code**: `service.yaml` is the SINGLE SOURCE OF TRUTH for all service enablement decisions
4. **Health-Driven Dependencies**: Service health checks for dependency management
5. **External Network Communication**: Cross-compose service communication
6. **Shared Storage**: Configuration and data sharing between layers
7. **Two-Phase Startup**: Platform infrastructure before application logic
8. **gRPC Architecture**: Microservices communication via gRPC servers
9. **Subdomain Routing**: Service isolation via nginx subdomain routing
10. **Database Migration Management**: Flyway-style schema versioning
11. **External Connector Integration**: PyAirbyte for data source connectivity
12. **Environment Variables for Configuration Only**: Env vars provide connection details, NOT service enablement

## Additional Features

### PyAirbyte Integration
- External data connector management
- Configurable connector settings via environment variables
- Cache database for synced data storage
- Support for multiple data sources
- Connector configurations in `app/data-platform-service/data-manager/external-connectors/`
- Utilities in `app/data-platform-service/data-manager/pyairbyte/utils/` for sync, cache management, Excel processing, MSSQL/MySQL sync, and event handling

### Database Schema Management
- Flyway-style migration system
- Versioned schema changes (V1-V6 currently)
- Automated database initialization via AppBase Init service
- Multi-schema support (hasura, metabase, superset, dagster)
- AppBase Init uses multi-stage Dockerfile with Flyway extracted from official image
- Node.js 24.11.1 LTS for initialization scripts

### Nginx Subdomain Routing
- `hasura.localhost` - GraphQL engine access
- `dagster.localhost` - Data orchestration (with basic auth)
- `superset.localhost` - Business intelligence dashboard
- `cloudbeaver.localhost` - Web database manager (CloudBeaver Community)
- Health check endpoint at `/health`

### Hardened Container Images

All platform services use security-hardened container images with zero critical and high vulnerabilities:

**Hasura GraphQL Engine:**
- **Image**: `hasura/graphql-engine:v2.48.6` (official image, upgraded from v2.48.4)
- **Vulnerabilities**: 0 Critical, 0 High, 2 Medium (unfixable base OS)
- **Improvement**: 94.4% vulnerability reduction (36 → 2 Medium)

**Metabase BI Tool:**
- **Image**: `appbase/metabase:hardened` (custom multi-stage build)
- **Base**: `metabase/metabase:v0.57.2` (upgraded from v0.56.12)
- **Dockerfile**: `platform/Dockerfile.metabase`
- **Vulnerabilities**: 0 Critical, 0 High, 5 Medium (unfixable base OS)
- **Security**: Fixed JAR dependencies via classpath precedence (5 vulnerabilities fixed at runtime)

**Apache Superset:**
- **Image**: `appbase/superset:source-build` (custom multi-stage build from source)
- **Base**: Ubuntu 24.04 LTS with Python 3.11
- **Dockerfile**: `platform/Dockerfile.superset`
- **Version**: Apache Superset 5.0.0
- **Vulnerabilities**: 0 Critical, 0 High, 2 Medium (unfixable base OS)
- **Size Reduction**: 68% smaller (2.74 GB → 866 MB)
- **Security**: Upgraded pyarrow to 17.0.0, cryptography to 43.0.3

**Dagster:**
- **Image**: Custom build from `platform/Dockerfile.dagster`
- **Base**: `python:3.11-slim` (multi-stage build)
- **Vulnerabilities**: 0 Critical, 0 High, 2 Medium (unfixable base OS)
- **Security**: Upgraded pip to 25.3+ (fixes CVE-2025-8869), minimal runtime dependencies

**Data Platform Service:**
- **Image**: `appbase/data-manager:multistage` (custom multi-stage build)
- **Base**: `python:3.11-slim` (multi-stage build)
- **Features**: Microsoft ODBC drivers 17 & 18, Dagster 1.10.21, DBT Core 1.9.0, PyAirbyte 0.28.0
- **Security**: Upgraded pip to 25.3+ (fixes CVE-2025-8869), staged dependency installation
- **Dockerfile**: `app/Dockerfile.data-platform-service`
- **Service Name**: `data-platform-service` (container name and service name)
- **Organization**: All components (data-manager, dagster_code, dbt_models, dbt_models_se) under `app/data-platform-service/`

**AppBase Init:**
- **Image**: Custom build from `app/Dockerfile.appbase-init` (multi-stage build)
- **Base**: Ubuntu 24.04 LTS
- **Features**: Node.js 24.11.1 LTS, Flyway (extracted from official image), Java 17
- **Purpose**: Application initialization, database migrations, Hasura metadata setup, BI tool configuration
- **Dockerfile**: `app/Dockerfile.appbase-init`

**Build Requirements:**
- All custom images must be built with `--platform linux/amd64` for production Linux compatibility
- Multi-stage builds minimize attack surface by removing build tools from final images
- All images tested and verified with Docker Scout vulnerability scanning

**Dagster Code Locations:**
- **bridgestone_data_sync** (port 4273): Bridgestone data synchronization pipeline
  - Assets: `hello_world_asset`, `sync_invoice_data`, `sync_credit_data`, `sync_wwi_invoices`
  - Jobs: `bridgestone_data_sync_job`, `sync_data_job`
- All code locations configured in `app/data-platform-service/data-manager/resources/dagster/code-locations.json`
- Platform workspace references all locations in `platform/workspace.yaml`

This architecture ensures scalable, maintainable, and production-ready deployment of the data platform services.

