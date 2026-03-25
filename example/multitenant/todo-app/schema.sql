-- Todo App — a simple productivity SaaS
-- Tables: users, todos, labels, todo_labels

CREATE TABLE users (
  id            SERIAL PRIMARY KEY,
  email         TEXT NOT NULL UNIQUE,
  first_name    TEXT NOT NULL,
  last_name     TEXT,
  plan          TEXT NOT NULL DEFAULT 'free',        -- free | pro | team
  timezone      TEXT NOT NULL DEFAULT 'UTC',
  signed_up_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_login_at TIMESTAMPTZ
);

CREATE TABLE todos (
  id          SERIAL PRIMARY KEY,
  user_id     INT NOT NULL REFERENCES users(id),
  title       TEXT NOT NULL,
  completed   BOOLEAN NOT NULL DEFAULT false,
  due_date    DATE,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  completed_at TIMESTAMPTZ
);

CREATE TABLE labels (
  id      SERIAL PRIMARY KEY,
  user_id INT NOT NULL REFERENCES users(id),
  name    TEXT NOT NULL,
  color   TEXT NOT NULL DEFAULT '#6366f1'
);

CREATE TABLE todo_labels (
  todo_id  INT NOT NULL REFERENCES todos(id),
  label_id INT NOT NULL REFERENCES labels(id),
  PRIMARY KEY (todo_id, label_id)
);

-- Logical replication setup for pg2iceberg
ALTER TABLE users       REPLICA IDENTITY FULL;
ALTER TABLE todos       REPLICA IDENTITY FULL;
ALTER TABLE labels      REPLICA IDENTITY FULL;
ALTER TABLE todo_labels REPLICA IDENTITY FULL;

CREATE PUBLICATION pg2iceberg_pub FOR TABLE users, todos, labels, todo_labels;
