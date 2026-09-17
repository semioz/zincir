UPDATE events SET idempotency_key = NULL WHERE event_type = 'tool_call';
UPDATE events
SET idempotency_key = 'tool_call:' || json_extract(payload, '$.call_id')
WHERE event_type = 'tool_call';
