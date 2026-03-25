-- CashCat — a hypothetical neobank
-- Tables: customers, accounts, transactions, cards, support_tickets

CREATE TABLE customers (
  id              SERIAL PRIMARY KEY,
  email           TEXT NOT NULL UNIQUE,
  first_name      TEXT NOT NULL,
  last_name       TEXT,
  phone           TEXT,
  date_of_birth   DATE,
  kyc_status      TEXT NOT NULL DEFAULT 'pending',     -- pending | verified | rejected
  tier            TEXT NOT NULL DEFAULT 'standard',    -- standard | plus | premium
  referral_code   TEXT UNIQUE,
  referred_by     INT REFERENCES customers(id),
  signed_up_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_active_at  TIMESTAMPTZ
);

CREATE TABLE accounts (
  id            SERIAL PRIMARY KEY,
  customer_id   INT NOT NULL REFERENCES customers(id),
  type          TEXT NOT NULL DEFAULT 'checking',      -- checking | savings | investment
  currency      TEXT NOT NULL DEFAULT 'USD',
  balance_cents BIGINT NOT NULL DEFAULT 0,
  status        TEXT NOT NULL DEFAULT 'active',        -- active | frozen | closed
  opened_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE transactions (
  id              SERIAL PRIMARY KEY,
  account_id      INT NOT NULL REFERENCES accounts(id),
  type            TEXT NOT NULL,                        -- deposit | withdrawal | transfer | purchase | refund | fee
  amount_cents    BIGINT NOT NULL,
  currency        TEXT NOT NULL DEFAULT 'USD',
  merchant_name   TEXT,                                 -- null for non-purchase transactions
  category        TEXT,                                 -- groceries | dining | transport | entertainment | utilities | transfer | other
  description     TEXT,
  status          TEXT NOT NULL DEFAULT 'completed',    -- pending | completed | failed | reversed
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE cards (
  id            SERIAL PRIMARY KEY,
  customer_id   INT NOT NULL REFERENCES customers(id),
  account_id    INT NOT NULL REFERENCES accounts(id),
  type          TEXT NOT NULL DEFAULT 'virtual',       -- virtual | physical
  last_four     TEXT NOT NULL,
  status        TEXT NOT NULL DEFAULT 'active',        -- active | frozen | cancelled
  daily_limit_cents INT NOT NULL DEFAULT 500000,       -- $5,000 default
  issued_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE support_tickets (
  id            SERIAL PRIMARY KEY,
  customer_id   INT NOT NULL REFERENCES customers(id),
  subject       TEXT NOT NULL,
  category      TEXT NOT NULL,                          -- billing | fraud | account | card | general
  status        TEXT NOT NULL DEFAULT 'open',           -- open | in_progress | resolved | closed
  priority      TEXT NOT NULL DEFAULT 'normal',         -- low | normal | high | urgent
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  resolved_at   TIMESTAMPTZ
);

-- Logical replication setup for pg2iceberg
ALTER TABLE customers       REPLICA IDENTITY FULL;
ALTER TABLE accounts        REPLICA IDENTITY FULL;
ALTER TABLE transactions    REPLICA IDENTITY FULL;
ALTER TABLE cards           REPLICA IDENTITY FULL;
ALTER TABLE support_tickets REPLICA IDENTITY FULL;

CREATE PUBLICATION pg2iceberg_pub FOR TABLE customers, accounts, transactions, cards, support_tickets;
