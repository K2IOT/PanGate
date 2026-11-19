-- Review 2026-11-19 (F3): the delivered mark a member who hides reads may be seen at. Raised only by a real
-- chat.delivered (or by sending), written USING TIMESTAMP <mark> like the other marks; ALTER ... ADD tolerates a rerun.
ALTER TABLE ${keyspace}.conv_marks ADD shown_delivered_seq bigint;
