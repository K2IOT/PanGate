-- CH3 spec §3.2 (conversation state). ${keyspace} is replaced by SchemaMigrator; ALTER ... ADD tolerates a rerun.
-- Every mark column (delivered_seq, read_seq) is only written USING TIMESTAMP <mark>, so marks never go back.
ALTER TABLE ${keyspace}.user_convs ADD delivered_seq bigint;
-- state_at: wall time of the row's last state change (message, own mark, DM peer mark), written with its own timestamp
ALTER TABLE ${keyspace}.user_convs ADD state_at bigint;
-- per-member marks of a conversation; never deleted (a members-row delete would shadow seq-timestamped cells)
CREATE TABLE IF NOT EXISTS ${keyspace}.conv_marks (
  conv_id text, user_id text, delivered_seq bigint, read_seq bigint,
  PRIMARY KEY (conv_id, user_id)
);
CREATE TABLE IF NOT EXISTS ${keyspace}.user_settings (
  user_id text PRIMARY KEY, hide_presence boolean, hide_read boolean, updated_at bigint
);
CREATE TABLE IF NOT EXISTS ${keyspace}.last_seen (
  user_id text PRIMARY KEY, last_seen_ms bigint
);
