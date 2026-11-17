-- Owner decision 2026-11-17 (CH3 spec §6.3): presence counts a DM only once the watched user sent a message there,
-- and not a group where the viewer added the watched user (until that user sends a message there).
-- last_sent_seq: written USING TIMESTAMP <seq> like the other mark columns. ALTER ... ADD tolerates a rerun.
ALTER TABLE ${keyspace}.conv_marks ADD last_sent_seq bigint;
-- added_by: who added this member; the row is deleted on leave and written again on a re-add
ALTER TABLE ${keyspace}.members ADD added_by text;
