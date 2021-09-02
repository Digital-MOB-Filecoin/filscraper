ALTER TABLE fil_messages ADD COLUMN "Cid" text;
ALTER TABLE fil_blocks ADD COLUMN "msg_cid" BOOLEAN;
CREATE INDEX IF NOT EXISTS idx_fil_messages_CID2 ON fil_messages("Cid");