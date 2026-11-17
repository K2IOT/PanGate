-- Owner decision 2026-11-17 (CH3 spec §6.3): presence counts a DM or a group only once the watched user sent a
-- message there.
-- last_sent_seq: written USING TIMESTAMP <seq> like the other mark columns. ALTER ... ADD tolerates a rerun.
ALTER TABLE ${keyspace}.conv_marks ADD last_sent_seq bigint;
