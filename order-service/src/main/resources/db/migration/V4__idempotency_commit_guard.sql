-- A temporary claim may exist only inside its creating transaction. Check the final row at COMMIT.
CREATE FUNCTION check_idempotency_complete() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE incomplete boolean;
BEGIN
    EXECUTE format('SELECT EXISTS (SELECT 1 FROM %I.request_idempotency
        WHERE customer_id=$1 AND operation=$2 AND idempotency_key=$3
        AND (resource_id IS NULL OR response_body IS NULL OR response_body=''null''::jsonb
             OR response_status NOT BETWEEN 200 AND 299))', TG_TABLE_SCHEMA)
        INTO incomplete USING NEW.customer_id, NEW.operation, NEW.idempotency_key;
    IF incomplete THEN
        RAISE EXCEPTION 'Incomplete idempotency result' USING ERRCODE='23514';
    END IF;
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER idempotency_complete_at_commit
AFTER INSERT OR UPDATE ON request_idempotency DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION check_idempotency_complete();
