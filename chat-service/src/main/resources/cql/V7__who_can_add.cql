-- Review 2026-11-19 (F7): whoCanAdd = nobody; null = everyone. ALTER ... ADD tolerates a rerun.
ALTER TABLE ${keyspace}.user_settings ADD refuse_adds boolean;
