-- Upgrade pgmq from 1.6.1 to 1.6.2
-- replace pop function with new version that adds multi-message pop

DROP FUNCTION pgmq.pop(queue_name TEXT);    
CREATE FUNCTION pgmq.pop(queue_name TEXT, qty INTEGER)
RETURNS SETOF pgmq.message_record AS $$
DECLARE
    sql TEXT;
    result pgmq.message_record;
    qtable TEXT := pgmq.format_table_name(queue_name, 'q');
BEGIN
    sql := FORMAT(
        $QUERY$
        WITH cte AS
            (
                SELECT msg_id
                FROM pgmq.%I
                WHERE vt <= clock_timestamp()
                ORDER BY msg_id ASC
                LIMIT $1
                FOR UPDATE SKIP LOCKED
            )
        DELETE from pgmq.%I
        WHERE msg_id IN (select msg_id from cte)
        RETURNING *;
        $QUERY$,
        qtable, qtable
    );
    RETURN QUERY EXECUTE sql USING qty;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION pgmq.pop(queue_name TEXT)
RETURNS SETOF pgmq.message_record AS $$
BEGIN
    RETURN QUERY SELECT * FROM pgmq.pop(queue_name, 1);
END;
$$ LANGUAGE plpgsql;