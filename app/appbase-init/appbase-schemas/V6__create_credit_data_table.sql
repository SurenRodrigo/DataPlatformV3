-- Credit Data Table
-- This table stores credit line items with financial and dimensional data
CREATE TABLE IF NOT EXISTS credit_data (
    id SERIAL PRIMARY KEY,
    invoice_number INTEGER NOT NULL,
    customer_name VARCHAR(255) NOT NULL,
    item_code VARCHAR(100) NOT NULL,
    seller_id INTEGER NOT NULL,
    quantity NUMERIC(10,2) NOT NULL,
    line_total NUMERIC(12,2) NOT NULL,
    gross_profit NUMERIC(12,2) NOT NULL,
    customer_code VARCHAR(100),
    items_group_name VARCHAR(255),
    posted_date DATE NOT NULL,
    end_of_month_bucket DATE,
    item_category VARCHAR(255),
    year INTEGER NOT NULL,
    customer_group VARCHAR(255),
    cohort VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for common query patterns
-- Index on invoice_number for invoice lookups
CREATE INDEX IF NOT EXISTS idx_credit_data_invoice_number 
ON credit_data (invoice_number);

-- Index on posted_date for date range queries
CREATE INDEX IF NOT EXISTS idx_credit_data_posted_date 
ON credit_data (posted_date);

-- Index on customer_code for customer-based queries
CREATE INDEX IF NOT EXISTS idx_credit_data_customer_code 
ON credit_data (customer_code);

-- Index on year for year-based queries
CREATE INDEX IF NOT EXISTS idx_credit_data_year 
ON credit_data (year);

-- Composite index on year and posted_date for common reporting queries
CREATE INDEX IF NOT EXISTS idx_credit_data_year_posted_date 
ON credit_data (year, posted_date);

-- Index on seller_id for seller-based queries
CREATE INDEX IF NOT EXISTS idx_credit_data_seller_id 
ON credit_data (seller_id);

-- Index on item_code for item-based queries
CREATE INDEX IF NOT EXISTS idx_credit_data_item_code 
ON credit_data (item_code);

-- Index on end_of_month_bucket for monthly reporting
CREATE INDEX IF NOT EXISTS idx_credit_data_end_of_month_bucket 
ON credit_data (end_of_month_bucket);

-- Composite index on invoice_number and posted_date for invoice reconciliation queries
CREATE INDEX IF NOT EXISTS idx_credit_data_invoice_number_posted_date 
ON credit_data (invoice_number, posted_date);
