-- Offline notifications (CH4 spec §3, §4). ALTER ... ADD tolerates a rerun.
-- the sender's name in notifications, from account.users (§3.2)
ALTER TABLE ${keyspace}.users ADD display_name text;
-- mute per conversation (§3.3): 0 = not muted, Long.MAX_VALUE = for ever; on members for the channel notifier's
-- paging, on user_convs for the conversation list
ALTER TABLE ${keyspace}.members ADD muted_until bigint;
ALTER TABLE ${keyspace}.user_convs ADD muted_until bigint;
-- hide the preview of notifications (§3.4)
ALTER TABLE ${keyspace}.user_settings ADD hide_preview boolean;
-- where a channel notification job stopped (§4.2)
CREATE TABLE IF NOT EXISTS ${keyspace}.channel_notify_progress (
  conv_id text PRIMARY KEY, conv_seq bigint, last_user_id text
) WITH default_time_to_live = 86400;
