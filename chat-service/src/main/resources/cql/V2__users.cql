-- account spec §8: users chat-service knows from account.users. ${keyspace} is replaced by SchemaMigrator.
CREATE TABLE IF NOT EXISTS ${keyspace}.users (
  user_id text PRIMARY KEY, status text, updated_at bigint
);
