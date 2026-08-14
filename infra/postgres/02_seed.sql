\set ON_ERROR_STOP on

CREATE OR REPLACE FUNCTION control.apply_demo_batch(p_batch_id bigint)
RETURNS boolean
LANGUAGE plpgsql
AS $$
DECLARE
    batch_start timestamptz;
    batch_end timestamptz;
    mutation_count integer;
    source_checksum text;
BEGIN
    IF p_batch_id NOT BETWEEN 1 AND 3 THEN
        RAISE EXCEPTION 'unknown deterministic demo batch %', p_batch_id;
    END IF;

    PERFORM pg_advisory_xact_lock(hashtext('millrace.demo.batch'), p_batch_id::integer);

    IF EXISTS (
        SELECT 1
        FROM control.source_batch
        WHERE batch_id = p_batch_id AND state = 'completed'
    ) THEN
        RETURN false;
    END IF;

    batch_start := timestamptz '2025-01-01 00:00:00+00'
        + ((p_batch_id - 1) * interval '1 day');
    batch_end := batch_start + interval '1 day';

    INSERT INTO control.source_batch (
        batch_id, source_name, state, data_interval_start, data_interval_end
    ) VALUES (
        p_batch_id, 'deterministic-retail-demo', 'loading', batch_start, batch_end
    )
    ON CONFLICT (batch_id) DO UPDATE
    SET source_name = EXCLUDED.source_name,
        state = 'loading',
        data_interval_start = EXCLUDED.data_interval_start,
        data_interval_end = EXCLUDED.data_interval_end,
        row_count = NULL,
        checksum = NULL,
        completed_at = NULL;

    PERFORM set_config('millrace.batch_id', p_batch_id::text, true);

    IF p_batch_id = 1 THEN
        INSERT INTO retail.customers (
            customer_id, email, first_name, last_name, status, updated_at, batch_id
        ) VALUES
            (1001, 'ada@example.test', 'Ada', 'Lovelace', 'active',
             '2025-01-01 08:00:00+00', p_batch_id),
            (1002, 'grace@example.test', 'Grace', 'Hopper', 'active',
             '2025-01-01 08:01:00+00', p_batch_id);

        INSERT INTO retail.products (
            product_id, sku, name, category, unit_price, active, updated_at, batch_id
        ) VALUES
            (2001, 'BOOK-001', 'Analytical Engine Notes', 'books', 19.95, true,
             '2025-01-01 08:02:00+00', p_batch_id),
            (2002, 'MUG-001', 'Compiler Mug', 'home', 22.50, true,
             '2025-01-01 08:03:00+00', p_batch_id),
            (2003, 'TEE-001', 'Data Flow Shirt', 'apparel', 29.00, true,
             '2025-01-01 08:04:00+00', p_batch_id);

        INSERT INTO retail.orders (
            order_id, customer_id, ordered_at, status, updated_at, batch_id
        ) VALUES
            (5001, 1001, '2025-01-01 09:00:00+00', 'paid',
             '2025-01-01 09:00:00+00', p_batch_id),
            (5002, 1002, '2025-01-01 09:10:00+00', 'pending',
             '2025-01-01 09:10:00+00', p_batch_id);

        INSERT INTO retail.order_items (
            order_id, line_number, product_id, quantity, unit_price, updated_at, batch_id
        ) VALUES
            (5001, 1, 2001, 1, 19.95, '2025-01-01 09:00:00+00', p_batch_id),
            (5001, 2, 2002, 2, 22.50, '2025-01-01 09:00:00+00', p_batch_id),
            (5002, 1, 2003, 1, 29.00, '2025-01-01 09:10:00+00', p_batch_id);
    ELSIF p_batch_id = 2 THEN
        UPDATE retail.customers
        SET email = 'grace.hopper@example.test',
            updated_at = '2025-01-02 08:00:00+00',
            batch_id = p_batch_id
        WHERE customer_id = 1002;

        INSERT INTO retail.customers (
            customer_id, email, first_name, last_name, status, updated_at, batch_id
        ) VALUES (
            1003, 'katherine@example.test', 'Katherine', 'Johnson', 'active',
            '2025-01-02 08:01:00+00', p_batch_id
        );

        UPDATE retail.products
        SET unit_price = 24.00,
            updated_at = '2025-01-02 08:02:00+00',
            batch_id = p_batch_id
        WHERE product_id = 2002;

        INSERT INTO retail.products (
            product_id, sku, name, category, unit_price, active, updated_at, batch_id
        ) VALUES (
            2004, 'PIN-001', 'KRaft Enamel Pin', 'accessories', 8.50, true,
            '2025-01-02 08:03:00+00', p_batch_id
        );

        INSERT INTO retail.orders (
            order_id, customer_id, ordered_at, status, updated_at, batch_id
        ) VALUES (
            5003, 1003, '2025-01-02 10:00:00+00', 'paid',
            '2025-01-02 10:00:00+00', p_batch_id
        );

        INSERT INTO retail.order_items (
            order_id, line_number, product_id, quantity, unit_price, updated_at, batch_id
        ) VALUES
            (5003, 1, 2002, 1, 24.00, '2025-01-02 10:00:00+00', p_batch_id),
            (5003, 2, 2004, 3, 8.50, '2025-01-02 10:00:00+00', p_batch_id);
    ELSE
        UPDATE retail.orders
        SET status = 'shipped',
            updated_at = '2025-01-03 08:00:00+00',
            batch_id = p_batch_id
        WHERE order_id = 5001;

        UPDATE retail.products
        SET active = false,
            updated_at = '2025-01-03 08:01:00+00',
            batch_id = p_batch_id
        WHERE product_id = 2003;

        UPDATE retail.order_items
        SET updated_at = '2025-01-03 08:02:00+00',
            batch_id = p_batch_id
        WHERE order_id = 5002;

        UPDATE retail.orders
        SET updated_at = '2025-01-03 08:02:00+00',
            batch_id = p_batch_id
        WHERE order_id = 5002;

        DELETE FROM retail.orders WHERE order_id = 5002;

        INSERT INTO retail.orders (
            order_id, customer_id, ordered_at, status, updated_at, batch_id
        ) VALUES (
            5004, 1002, '2025-01-03 11:00:00+00', 'paid',
            '2025-01-03 11:00:00+00', p_batch_id
        );

        INSERT INTO retail.order_items (
            order_id, line_number, product_id, quantity, unit_price, updated_at, batch_id
        ) VALUES
            (5004, 1, 2001, 2, 19.95, '2025-01-03 11:00:00+00', p_batch_id),
            (5004, 2, 2004, 1, 8.50, '2025-01-03 11:00:00+00', p_batch_id);
    END IF;

    SELECT count(*)::integer
    INTO mutation_count
    FROM (
        SELECT batch_id FROM source_history.customers
        UNION ALL
        SELECT batch_id FROM source_history.products
        UNION ALL
        SELECT batch_id FROM source_history.orders
        UNION ALL
        SELECT batch_id FROM source_history.order_items
    ) AS mutations
    WHERE batch_id = p_batch_id;

    SELECT md5(string_agg(record_value, '|' ORDER BY record_value))
    INTO source_checksum
    FROM (
        SELECT format(
            'customer:%s:%s:%s:%s:%s:%s',
            customer_id, email, first_name, last_name, status, batch_id
        ) AS record_value
        FROM retail.customers
        UNION ALL
        SELECT format(
            'product:%s:%s:%s:%s:%s:%s:%s',
            product_id, sku, name, category, unit_price, active, batch_id
        )
        FROM retail.products
        UNION ALL
        SELECT format(
            'order:%s:%s:%s:%s:%s',
            order_id, customer_id, ordered_at, status, batch_id
        )
        FROM retail.orders
        UNION ALL
        SELECT format(
            'item:%s:%s:%s:%s:%s:%s',
            order_id, line_number, product_id, quantity, unit_price, batch_id
        )
        FROM retail.order_items
    ) AS source_records;

    UPDATE control.source_batch
    SET state = 'completed',
        row_count = mutation_count,
        checksum = source_checksum,
        completed_at = batch_end
    WHERE batch_id = p_batch_id;

    RETURN true;
END;
$$;

SELECT control.apply_demo_batch(batch_id)
FROM generate_series(1, 1) AS batches(batch_id);
