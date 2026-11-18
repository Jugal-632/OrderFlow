DO $$
DECLARE
    s text;
BEGIN
    FOR s IN SELECT unnest(ARRAY['auth_schema', 'order_schema', 'inventory_schema', 'notification_schema']) LOOP
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS %I.flyway_schema_history (
                installed_rank INT NOT NULL PRIMARY KEY,
                version VARCHAR(50),
                description VARCHAR(200) NOT NULL,
                type VARCHAR(20) NOT NULL,
                script VARCHAR(1000) NOT NULL,
                checksum INT,
                installed_by VARCHAR(100) NOT NULL,
                installed_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                execution_time INT NOT NULL,
                success BOOLEAN NOT NULL
            );
            INSERT INTO %I.flyway_schema_history 
            (installed_rank, version, description, type, script, checksum, installed_by, installed_on, execution_time, success)
            VALUES (1, ''1'', ''baseline'', ''BASELINE'', ''<< Flyway Baseline >>'', NULL, ''orderflow'', NOW(), 0, TRUE)
            ON CONFLICT (installed_rank) DO NOTHING;
        ', s, s);
    END LOOP;
END $$;
