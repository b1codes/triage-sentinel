-- Records how each fingerprint was derived, so grouping quality can be tuned
-- from evidence. M1 spends $0.00, which makes it a free observation period:
-- after real traffic the dashboard can show which strategy a project fell
-- back to and which frames grouped what, and source_roots can be tuned before
-- M2 begins spending against these groupings.
--
-- Forward-only: never edit this file after it has been applied anywhere.

-- One of: source_roots | denylist | all_frames | workflow | no_frames.
-- 'unknown' is the default only for rows written before this migration.
ALTER TABLE fingerprints ADD COLUMN strategy TEXT NOT NULL DEFAULT 'unknown';

-- The normalised frames that produced the hash, as a JSON array of strings.
-- Without these it is impossible to tell why two errors collapsed, only that
-- they did.
ALTER TABLE fingerprints ADD COLUMN frames_json TEXT NOT NULL DEFAULT '[]';
