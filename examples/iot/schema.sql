CREATE TABLE site (
    id   INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE device (
    id      INTEGER PRIMARY KEY,
    site_id INTEGER NOT NULL REFERENCES site(id) ON DELETE CASCADE,
    name    TEXT NOT NULL,
    online  BOOLEAN NOT NULL DEFAULT 1
);

CREATE TABLE sensor (
    id        INTEGER PRIMARY KEY,
    device_id INTEGER NOT NULL REFERENCES device(id) ON DELETE CASCADE,
    kind      TEXT NOT NULL,          -- 'temp', 'humidity', ...
    max_value REAL                    -- alert threshold
);

CREATE TABLE reading (
    id        INTEGER PRIMARY KEY,
    sensor_id INTEGER NOT NULL REFERENCES sensor(id) ON DELETE CASCADE,
    value     REAL NOT NULL,
    at        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
