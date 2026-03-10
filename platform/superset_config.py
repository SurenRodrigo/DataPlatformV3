# Apache Superset Configuration File
# This file configures Superset for the Business Application Development Platform

import os

# =============================================================================
# DATABASE CONFIGURATION
# =============================================================================

# Superset metadata database (where Superset stores its own data)
SQLALCHEMY_DATABASE_URI = os.environ.get('SUPERSET_DATABASE_URI')

# SQLAlchemy Engine Options for PostgreSQL connection pooling
# Fixes psycopg2.DatabaseError: error with status PGRES_TUPLES_OK
# These settings ensure connections are validated and recycled properly
# Production-grade configuration:
# - pool_size: Sufficient for multiple workers (default 2 workers * 2 threads = 4 concurrent)
#   Formula: pool_size >= (workers * threads) + buffer (recommended: 2-3x for production)
#   With 2 workers * 2 threads = 4, pool_size of 20 provides 5x headroom
# - max_overflow: Additional connections during peak load (total max = pool_size + max_overflow)
# - pool_recycle: 30 minutes (1800s) - recommended for production to prevent stale connections
#   Should be less than database idle_in_transaction_session_timeout
# - pool_pre_ping: Essential for production to detect and replace stale connections
SQLALCHEMY_ENGINE_OPTIONS = {
    'pool_pre_ping': True,  # Verify connections before using them (fixes stale connection errors)
    'pool_recycle': 1800,  # Recycle connections after 30 minutes (production best practice)
    'pool_size': 20,  # Number of connections to maintain in the pool (production-grade)
    'max_overflow': 10,  # Maximum number of connections beyond pool_size (total max = 30)
    'pool_timeout': 30,  # Seconds to wait before giving up on getting a connection
    'connect_args': {
        'connect_timeout': 10,  # PostgreSQL connection timeout in seconds
        'options': '-c statement_timeout=30000'  # 30 second statement timeout
    }
}

# =============================================================================
# SECURITY CONFIGURATION
# =============================================================================

# Secret key for session encryption
SECRET_KEY = os.environ.get('SUPERSET_SECRET_KEY')

# Proxy fix disabled - direct access
ENABLE_PROXY_FIX = False

# Enable CORS for Hasura integration and cross-origin requests
ENABLE_CORS = True
CORS_OPTIONS = {
    'supports_credentials': True,
    'allow_headers': ['*'],
    'origins': ['*']
}

TALISMAN_ENABLED = False

# Disable CSRF protection for API endpoints (needed for automation)
WTF_CSRF_ENABLED = False

# =============================================================================
# AUTHENTICATION CONFIGURATION
# =============================================================================

# Authentication type: 1 = Database authentication (local users)
AUTH_TYPE = 1

# Default roles
AUTH_ROLE_ADMIN = 'Admin'
AUTH_ROLE_PUBLIC = 'Public'

# User registration settings
AUTH_USER_REGISTRATION = True
AUTH_USER_REGISTRATION_ROLE = 'Public'

# =============================================================================
# FEATURE FLAGS
# =============================================================================

# Enable modern Superset features
FEATURE_FLAGS = {
    'DASHBOARD_NATIVE_FILTERS': True,
    "DASHBOARD_CROSS_FILTERS": True,  # Enable cross-filtering between charts
    'DASHBOARD_NATIVE_FILTERS_SET': True,
    "DASHBOARD_FILTERS_EXPERIMENTAL": True,  # Experimental filter features
    'ENABLE_TEMPLATE_PROCESSING': True,
    "ENABLE_TEMPLATE_REMOVE_FILTERS": True,  # Allow removing filters via Jinja
    'SCHEDULED_QUERIES': True,
    'SQL_VALIDATORS_BY_ENGINE': True,
    'DASHBOARD_RBAC': True,
    'ENABLE_EXPLORE_JSON_CSRF_PROTECTION': False,
    'PRESTO_EXPAND_DATA': True,
    "TAGGING_SYSTEM": True,  # Enable tagging for datasets/charts/dashboards

}

# =============================================================================
# HTML SANITIZATION CONFIGURATION (Enhanced Safe Styling)
# =============================================================================

HTML_SANITIZATION = True

HTML_SANITIZATION_SCHEMA_EXTENSIONS = {
    "attributes": {
        "*": [
            # General styling and layout
            "style", "class", "id", "align",

            # Spacing and sizing
            "width", "height", "padding", "margin",

            # Borders and background
            "border", "border-color", "border-width", "border-style",
            "background", "background-color",

            # Text and colors
            "color", "font-size", "font-weight", "text-align",
            "line-height", "letter-spacing", "text-decoration",

            # Table formatting
            "cellpadding", "cellspacing", "colspan", "rowspan",

            # Data attributes (useful for custom logic)
            "data-*",  # Allows data-value, data-filter, etc.

            # ARIA accessibility
            "aria-label", "aria-labelledby", "aria-describedby",
            "aria-hidden", "role",
            
            # Title for tooltips
            "title",
        ],
        "img": ["src", "alt", "width", "height", "style", "class"],
        "a": ["href", "title", "target", "style", "class", "rel", "download", "type"],
        "table": [
            "border", "cellpadding", "cellspacing", "width", "style", "class",
        ],
        "tr": ["style", "class"],
        "td": ["style", "class", "colspan", "rowspan", "align",
            "headers",  # Accessibility - links to th
            "scope",  # row/col/rowgroup/colgroup
        ],
        "th": ["style", "class", "colspan", "rowspan", "align",
            "scope",  # row/col/rowgroup/colgroup
            "abbr",  # Abbreviation
        ],
        "span": ["style", "class"],
        "div": ["style", "class", "align"],
        "p": ["style", "class", "align"],
        "h1": ["style", "class", "align"],
        "h2": ["style", "class", "align"],
        "h3": ["style", "class", "align"],
        "h4": ["style", "class", "align"],
        "h5": ["style", "class", "align"],
        "h6": ["style", "class", "align"],

        # Lists
        "ul": ["style", "class", "type"],  # disc/circle/square
        "ol": ["style", "class", "type", "start"],  # 1/A/a/I/i, start number
        "li": ["style", "class", "value"],

        # Buttons (if you want clickable elements)
        "button": [
            "style", "class", "type", "disabled",
            "title", "aria-label",
        ],
    },
    "tagNames": [
        "style", "div", "span", "p", "table", "tr", "td", "th",
        "a", "img", "h1", "h2", "h3", "h4", "h5", "h6",
        "ul", "ol", "li", "button",
    ],
}

# Maximum number of rows to display in a table
NATIVE_FILTER_DEFAULT_ROW_LIMIT = 10000

D3_FORMAT = {
    "decimal": ",",
    "thousands": " ",
    "grouping": [3],
    "currency": ["NOK", " "],
}
# D3_FORMAT = {
#     "decimal": ".",
#     "thousands": ",",
#     "grouping": [3],
#     "currency": ["NOK ", ""],  # optional; or ["", ""] if you don't want NOK prefix
# }   

# =============================================================================
# APPLICATION CONFIGURATION
# =============================================================================

# Disable example data loading
SUPERSET_LOAD_EXAMPLES = False

# Cache configuration (Redis not required for basic setup)
CACHE_CONFIG = {
    'CACHE_TYPE': 'SimpleCache',
    'CACHE_DEFAULT_TIMEOUT': 300
}

# Custom branding
APP_NAME = "Gjerde & Byhring AS"
APP_ICON = "/static/assets/images/superset_logo.png"
APP_ICON_WIDTH = 300

# Optional: Custom CSS for further customization
CSS_DEFAULT_THEME = "bootstrap.min.css"

# Custom navbar logo
LOGO_TARGET_PATH = None  # Set to a URL if you want the logo to be clickable
LOGO_TOOLTIP = "Gjerde & Byhring AS"
LOGO_RIGHT_TEXT = ""  # Text to show next to logo

# =============================================================================
# SQLLAB CONFIGURATION
# =============================================================================

# Configure synchronous query execution to avoid Celery/Redis dependencies
# Queries will run synchronously, which is simpler and works well for most use cases
SQLLAB_ASYNC_TIME_LIMIT_SEC = 0  # 0 means run synchronously
SQLLAB_TIMEOUT = 300  # 5 minutes timeout for queries

# Force synchronous execution
FEATURE_FLAGS.update({
    'SQLLAB_BACKEND_PERSISTENCE': False,
})

# =============================================================================
# DATABASE CONNECTIONS
# =============================================================================

# This section can be extended by appbase-init to automatically
# configure connections to the application database

# Application database connection template (configured by appbase-init)
APPBASE_DB_CONFIG = {
    'host': os.environ.get('SUPERSET_APPBASE_DB_HOST'),
    'port': os.environ.get('SUPERSET_APPBASE_DB_PORT'),
    'username': os.environ.get('SUPERSET_APPBASE_DB_USER'),
    'password': os.environ.get('SUPERSET_APPBASE_DB_PASSWORD'),
    'database': os.environ.get('SUPERSET_APPBASE_DB_NAME'),
}

# =============================================================================
# LOGGING CONFIGURATION
# =============================================================================

# Basic logging configuration
import logging
logging.basicConfig(level=logging.INFO)

# =============================================================================
# WEBSERVER CONFIGURATION
# =============================================================================

# Webserver settings
WEBSERVER_TIMEOUT = 60
SUPERSET_WEBSERVER_PORT = 8088

# =============================================================================
# EMAIL CONFIGURATION (Optional)
# =============================================================================

# Email configuration can be added here for notifications
# Currently disabled for simplicity
EMAIL_NOTIFICATIONS = False

# =============================================================================
# WEBSERVER URL CONFIGURATION
# =============================================================================

# Direct access configuration (no proxy)

# Base URL for direct access
SUPERSET_WEBSERVER_BASEURL = 'http://localhost:8088'

# Use HTTP protocol for local development
SUPERSET_WEBSERVER_PROTOCOL = 'http'
WEB_SERVER_DISABLE_AUTH_FORCE = True

# =============================================================================
# CUSTOM CONFIGURATION HOOKS
# =============================================================================

# This section allows for additional customization by the platform
# Any custom configuration can be added here by appbase-init

print("✅ Superset configuration loaded successfully")
print(f"✅ Database URI configured: {SQLALCHEMY_DATABASE_URI is not None}")
print(f"✅ Secret key configured: {SECRET_KEY is not None}")
print(f"✅ Application database configured: {all(APPBASE_DB_CONFIG.values())}") 
