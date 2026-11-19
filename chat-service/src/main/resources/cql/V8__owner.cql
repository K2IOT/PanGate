-- Batch 1 review I1: who owns a group or channel, changed only with an LWT (IF owner_id = <current>), so racing
-- transfers or a transfer racing the owner's leave never leave two owners or none. Null on rows created before:
-- the member row with role OWNER is the owner then. ALTER ... ADD tolerates a rerun.
ALTER TABLE ${keyspace}.conversations ADD owner_id text;
