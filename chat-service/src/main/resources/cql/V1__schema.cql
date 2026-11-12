-- chat spec §5. ${keyspace} is replaced by SchemaMigrator.
CREATE TABLE IF NOT EXISTS ${keyspace}.messages (
  conv_id text, bucket bigint, conv_seq bigint, msg_id text, sender_id text, client_msg_id text, text text,
  reply_to text, mentions list<text>, sent_at bigint,
  PRIMARY KEY ((conv_id, bucket), conv_seq)
) WITH CLUSTERING ORDER BY (conv_seq DESC);
CREATE TABLE IF NOT EXISTS ${keyspace}.conversations (
  conv_id text PRIMARY KEY, kind int, title text, created_by text, created_at bigint, last_seq bigint,
  last_msg_at bigint, member_count int, is_public boolean
);
CREATE TABLE IF NOT EXISTS ${keyspace}.members (
  conv_id text, user_id text, role text, joined_at bigint,
  PRIMARY KEY (conv_id, user_id)
);
CREATE TABLE IF NOT EXISTS ${keyspace}.user_convs (
  user_id text, conv_id text, kind int, last_seq bigint, read_seq bigint, last_msg_at bigint, muted boolean,
  PRIMARY KEY (user_id, conv_id)
);
CREATE TABLE IF NOT EXISTS ${keyspace}.dm_index (
  user_lo text, user_hi text, conv_id text,
  PRIMARY KEY ((user_lo, user_hi))
);
CREATE TABLE IF NOT EXISTS ${keyspace}.send_dedup (
  conv_id text, sender_id text, client_msg_id text, msg_id text, conv_seq bigint, sent_at bigint,
  PRIMARY KEY ((conv_id, sender_id, client_msg_id))
) WITH default_time_to_live = 86400;
