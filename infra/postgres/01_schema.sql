\set ON_ERROR_STOP on

CREATE SCHEMA IF NOT EXISTS control;
CREATE SCHEMA IF NOT EXISTS retail;
CREATE SCHEMA IF NOT EXISTS audit;
CREATE SCHEMA IF NOT EXISTS source_history;

CREATE TABLE control.source_batch (
    batch_id bigint PRIMARY KEY,
    source_name text NOT NULL,
    state text NOT NULL CHECK (state IN ('loading', 'completed', 'failed')),
    data_interval_start timestamptz NOT NULL,
    data_interval_end timestamptz NOT NULL,
    row_count integer,
    checksum text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    CHECK (data_interval_end > data_interval_start),
    CHECK ((state = 'completed') = (completed_at IS NOT NULL))
);

CREATE TABLE control.pipeline_run (
    run_id uuid PRIMARY KEY,
    data_interval_start timestamptz NOT NULL,
    data_interval_end timestamptz NOT NULL,
    batch_id bigint NOT NULL REFERENCES control.source_batch (batch_id),
    state text NOT NULL CHECK (
        state IN ('created', 'ingesting', 'transforming', 'validating', 'published', 'failed')
    ),
    candidate_schema text NOT NULL,
    failure_reason text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (data_interval_start, data_interval_end, batch_id),
    CHECK (data_interval_end > data_interval_start),
    CHECK ((state = 'failed') = (failure_reason IS NOT NULL))
);

CREATE TABLE control.reconciliation_check (
    run_id uuid NOT NULL REFERENCES control.pipeline_run (run_id),
    check_name text NOT NULL,
    check_type text NOT NULL CHECK (check_type IN ('row_count', 'checksum', 'aggregate')),
    passed boolean NOT NULL,
    expected_value jsonb NOT NULL,
    actual_value jsonb NOT NULL,
    details jsonb NOT NULL DEFAULT '{}'::jsonb,
    checked_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (run_id, check_name)
);

CREATE TABLE control.publication (
    publication_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id uuid NOT NULL UNIQUE REFERENCES control.pipeline_run (run_id),
    candidate_schema text NOT NULL,
    published_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE audit.pipeline_event (
    event_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id uuid REFERENCES control.pipeline_run (run_id),
    event_type text NOT NULL,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    occurred_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE audit.dead_letter_event (
    dead_letter_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    topic text NOT NULL,
    partition_id integer NOT NULL,
    offset_id bigint NOT NULL,
    payload jsonb,
    reason_code text NOT NULL,
    reason_detail text,
    occurred_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (topic, partition_id, offset_id)
);

CREATE TABLE retail.customers (
    customer_id bigint PRIMARY KEY,
    email text NOT NULL UNIQUE,
    first_name text NOT NULL,
    last_name text NOT NULL,
    status text NOT NULL CHECK (status IN ('active', 'inactive')),
    updated_at timestamptz NOT NULL,
    batch_id bigint NOT NULL REFERENCES control.source_batch (batch_id)
);

CREATE TABLE retail.products (
    product_id bigint PRIMARY KEY,
    sku text NOT NULL UNIQUE,
    name text NOT NULL,
    category text NOT NULL,
    unit_price numeric(12, 2) NOT NULL CHECK (unit_price >= 0),
    active boolean NOT NULL,
    updated_at timestamptz NOT NULL,
    batch_id bigint NOT NULL REFERENCES control.source_batch (batch_id)
);

CREATE TABLE retail.orders (
    order_id bigint PRIMARY KEY,
    customer_id bigint NOT NULL REFERENCES retail.customers (customer_id),
    ordered_at timestamptz NOT NULL,
    status text NOT NULL CHECK (status IN ('pending', 'paid', 'shipped', 'cancelled')),
    updated_at timestamptz NOT NULL,
    batch_id bigint NOT NULL REFERENCES control.source_batch (batch_id)
);

CREATE TABLE retail.order_items (
    order_id bigint NOT NULL REFERENCES retail.orders (order_id) ON DELETE CASCADE,
    line_number integer NOT NULL CHECK (line_number > 0),
    product_id bigint NOT NULL REFERENCES retail.products (product_id),
    quantity integer NOT NULL CHECK (quantity > 0),
    unit_price numeric(12, 2) NOT NULL CHECK (unit_price >= 0),
    updated_at timestamptz NOT NULL,
    batch_id bigint NOT NULL REFERENCES control.source_batch (batch_id),
    PRIMARY KEY (order_id, line_number)
);

ALTER TABLE retail.customers REPLICA IDENTITY FULL;
ALTER TABLE retail.products REPLICA IDENTITY FULL;
ALTER TABLE retail.orders REPLICA IDENTITY FULL;
ALTER TABLE retail.order_items REPLICA IDENTITY FULL;

CREATE TABLE source_history.customers (
    history_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    operation char(1) NOT NULL CHECK (operation IN ('I', 'U', 'D')),
    is_deleted boolean GENERATED ALWAYS AS (operation = 'D') STORED,
    batch_id bigint NOT NULL REFERENCES control.source_batch (batch_id),
    captured_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    customer_id bigint NOT NULL,
    email text NOT NULL,
    first_name text NOT NULL,
    last_name text NOT NULL,
    status text NOT NULL,
    updated_at timestamptz NOT NULL
);

CREATE TABLE source_history.products (
    history_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    operation char(1) NOT NULL CHECK (operation IN ('I', 'U', 'D')),
    is_deleted boolean GENERATED ALWAYS AS (operation = 'D') STORED,
    batch_id bigint NOT NULL REFERENCES control.source_batch (batch_id),
    captured_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    product_id bigint NOT NULL,
    sku text NOT NULL,
    name text NOT NULL,
    category text NOT NULL,
    unit_price numeric(12, 2) NOT NULL,
    active boolean NOT NULL,
    updated_at timestamptz NOT NULL
);

CREATE TABLE source_history.orders (
    history_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    operation char(1) NOT NULL CHECK (operation IN ('I', 'U', 'D')),
    is_deleted boolean GENERATED ALWAYS AS (operation = 'D') STORED,
    batch_id bigint NOT NULL REFERENCES control.source_batch (batch_id),
    captured_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    order_id bigint NOT NULL,
    customer_id bigint NOT NULL,
    ordered_at timestamptz NOT NULL,
    status text NOT NULL,
    updated_at timestamptz NOT NULL
);

CREATE TABLE source_history.order_items (
    history_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    operation char(1) NOT NULL CHECK (operation IN ('I', 'U', 'D')),
    is_deleted boolean GENERATED ALWAYS AS (operation = 'D') STORED,
    batch_id bigint NOT NULL REFERENCES control.source_batch (batch_id),
    captured_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    order_id bigint NOT NULL,
    line_number integer NOT NULL,
    product_id bigint NOT NULL,
    quantity integer NOT NULL,
    unit_price numeric(12, 2) NOT NULL,
    updated_at timestamptz NOT NULL
);

CREATE INDEX customers_history_cutoff
    ON source_history.customers (customer_id, batch_id DESC, history_id DESC);
CREATE INDEX products_history_cutoff
    ON source_history.products (product_id, batch_id DESC, history_id DESC);
CREATE INDEX orders_history_cutoff
    ON source_history.orders (order_id, batch_id DESC, history_id DESC);
CREATE INDEX order_items_history_cutoff
    ON source_history.order_items (order_id, line_number, batch_id DESC, history_id DESC);

CREATE OR REPLACE FUNCTION control.active_batch_id()
RETURNS bigint
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    configured_batch text;
    resolved_batch bigint;
BEGIN
    configured_batch := current_setting('millrace.batch_id', true);
    IF configured_batch IS NULL OR configured_batch = '' THEN
        RAISE EXCEPTION 'millrace.batch_id must be set for retail mutations';
    END IF;

    BEGIN
        resolved_batch := configured_batch::bigint;
    EXCEPTION
        WHEN invalid_text_representation THEN
            RAISE EXCEPTION 'millrace.batch_id must be a bigint';
    END;

    IF NOT EXISTS (
        SELECT 1
        FROM control.source_batch
        WHERE batch_id = resolved_batch AND state = 'loading'
    ) THEN
        RAISE EXCEPTION 'batch % is not in loading state', resolved_batch;
    END IF;

    RETURN resolved_batch;
END;
$$;

CREATE OR REPLACE FUNCTION audit.reject_history_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION '% is immutable', TG_TABLE_NAME;
END;
$$;

CREATE TRIGGER customers_history_immutable
BEFORE UPDATE OR DELETE ON source_history.customers
FOR EACH ROW EXECUTE FUNCTION audit.reject_history_mutation();

CREATE TRIGGER products_history_immutable
BEFORE UPDATE OR DELETE ON source_history.products
FOR EACH ROW EXECUTE FUNCTION audit.reject_history_mutation();

CREATE TRIGGER orders_history_immutable
BEFORE UPDATE OR DELETE ON source_history.orders
FOR EACH ROW EXECUTE FUNCTION audit.reject_history_mutation();

CREATE TRIGGER order_items_history_immutable
BEFORE UPDATE OR DELETE ON source_history.order_items
FOR EACH ROW EXECUTE FUNCTION audit.reject_history_mutation();

CREATE OR REPLACE FUNCTION audit.capture_customer()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    row_data retail.customers%ROWTYPE;
    event_batch bigint;
BEGIN
    event_batch := control.active_batch_id();
    row_data := CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
    IF TG_OP <> 'DELETE' AND row_data.batch_id <> event_batch THEN
        RAISE EXCEPTION 'row batch % does not match active batch %', row_data.batch_id, event_batch;
    END IF;
    INSERT INTO source_history.customers (
        operation, batch_id, customer_id, email, first_name, last_name, status, updated_at
    ) VALUES (
        left(TG_OP, 1), event_batch, row_data.customer_id, row_data.email,
        row_data.first_name, row_data.last_name, row_data.status, row_data.updated_at
    );
    RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END;
$$;

CREATE OR REPLACE FUNCTION audit.capture_product()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    row_data retail.products%ROWTYPE;
    event_batch bigint;
BEGIN
    event_batch := control.active_batch_id();
    row_data := CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
    IF TG_OP <> 'DELETE' AND row_data.batch_id <> event_batch THEN
        RAISE EXCEPTION 'row batch % does not match active batch %', row_data.batch_id, event_batch;
    END IF;
    INSERT INTO source_history.products (
        operation, batch_id, product_id, sku, name, category, unit_price, active, updated_at
    ) VALUES (
        left(TG_OP, 1), event_batch, row_data.product_id, row_data.sku, row_data.name,
        row_data.category, row_data.unit_price, row_data.active, row_data.updated_at
    );
    RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END;
$$;

CREATE OR REPLACE FUNCTION audit.capture_order()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    row_data retail.orders%ROWTYPE;
    event_batch bigint;
BEGIN
    event_batch := control.active_batch_id();
    row_data := CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
    IF TG_OP <> 'DELETE' AND row_data.batch_id <> event_batch THEN
        RAISE EXCEPTION 'row batch % does not match active batch %', row_data.batch_id, event_batch;
    END IF;
    INSERT INTO source_history.orders (
        operation, batch_id, order_id, customer_id, ordered_at, status, updated_at
    ) VALUES (
        left(TG_OP, 1), event_batch, row_data.order_id, row_data.customer_id,
        row_data.ordered_at, row_data.status, row_data.updated_at
    );
    RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END;
$$;

CREATE OR REPLACE FUNCTION audit.capture_order_item()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    row_data retail.order_items%ROWTYPE;
    event_batch bigint;
BEGIN
    event_batch := control.active_batch_id();
    row_data := CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
    IF TG_OP <> 'DELETE' AND row_data.batch_id <> event_batch THEN
        RAISE EXCEPTION 'row batch % does not match active batch %', row_data.batch_id, event_batch;
    END IF;
    INSERT INTO source_history.order_items (
        operation, batch_id, order_id, line_number, product_id, quantity, unit_price, updated_at
    ) VALUES (
        left(TG_OP, 1), event_batch, row_data.order_id, row_data.line_number,
        row_data.product_id, row_data.quantity, row_data.unit_price, row_data.updated_at
    );
    RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END;
$$;

CREATE TRIGGER capture_customers
AFTER INSERT OR UPDATE OR DELETE ON retail.customers
FOR EACH ROW EXECUTE FUNCTION audit.capture_customer();

CREATE TRIGGER capture_products
AFTER INSERT OR UPDATE OR DELETE ON retail.products
FOR EACH ROW EXECUTE FUNCTION audit.capture_product();

CREATE TRIGGER capture_orders
AFTER INSERT OR UPDATE OR DELETE ON retail.orders
FOR EACH ROW EXECUTE FUNCTION audit.capture_order();

CREATE TRIGGER capture_order_items
AFTER INSERT OR UPDATE OR DELETE ON retail.order_items
FOR EACH ROW EXECUTE FUNCTION audit.capture_order_item();
