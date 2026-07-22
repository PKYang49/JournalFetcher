CREATE TABLE feedback (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    week TEXT NOT NULL,
    identifier TEXT NOT NULL,
    pmid TEXT NOT NULL DEFAULT '',
    doi TEXT NOT NULL DEFAULT '',
    journal TEXT NOT NULL DEFAULT '',
    title TEXT NOT NULL DEFAULT '',
    verdict TEXT NOT NULL CHECK (verdict IN ('up', 'down')),
    note TEXT NOT NULL DEFAULT '',
    UNIQUE (week, identifier)
);

CREATE INDEX feedback_ts_idx ON feedback (ts);

CREATE TABLE appraisal_requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    week TEXT NOT NULL,
    identifier TEXT NOT NULL,
    pmid TEXT NOT NULL DEFAULT '',
    doi TEXT NOT NULL DEFAULT '',
    journal TEXT NOT NULL DEFAULT '',
    title TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'requested',
    note TEXT NOT NULL DEFAULT '',
    UNIQUE (week, identifier)
);

CREATE INDEX appraisal_requests_ts_idx ON appraisal_requests (ts);
CREATE INDEX appraisal_requests_status_idx ON appraisal_requests (status, ts);

CREATE TABLE daily_write_counters (
    day TEXT PRIMARY KEY,
    count INTEGER NOT NULL CHECK (count >= 0)
);
