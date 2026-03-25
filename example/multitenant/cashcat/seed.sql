-- Seed data for CashCat neobank

INSERT INTO customers (email, first_name, last_name, phone, date_of_birth, kyc_status, tier, referral_code, referred_by, signed_up_at, last_active_at) VALUES
  ('hasyimi+sarah@hasyimibahrudin.com',    'Sarah',  'Johnson',    '+1-555-1001', '1992-04-15', 'verified', 'premium',  'SARAH2025',  NULL, '2025-06-01 10:00:00+00', '2026-03-21 09:00:00+00'),
  ('hasyimi+mike@hasyimibahrudin.com',     'Mike',   'Chang',      '+1-555-1002', '1988-11-22', 'verified', 'plus',     'MIKE88',     NULL, '2025-08-10 14:00:00+00', '2026-03-20 16:00:00+00'),
  ('hasyimi+emma@hasyimibahrudin.com',     'Emma',   'Williams',   '+1-555-1003', '1995-07-03', 'verified', 'standard', 'EMMA95',     1,   '2025-10-01 09:00:00+00', '2026-03-19 20:00:00+00'),
  ('hasyimi+raj@hasyimibahrudin.com',      'Raj',    'Kapoor',     '+1-555-1004', '1990-01-30', 'verified', 'plus',     'RAJ90',      NULL, '2025-12-15 11:00:00+00', '2026-03-21 07:30:00+00'),
  ('hasyimi+lisa@hasyimibahrudin.com',     'Lisa',   'Anderson',   '+1-555-1005', '1998-09-12', 'verified', 'standard', 'LISA98',     1,   '2026-01-05 08:00:00+00', '2026-03-18 14:00:00+00'),
  ('hasyimi+yuki@hasyimibahrudin.com',     'Yuki',   'Tanaka',     '+1-555-1006', '1993-03-25', 'verified', 'standard', 'YUKI93',     2,   '2026-01-20 03:00:00+00', '2026-03-20 22:00:00+00'),
  ('hasyimi+alex@hasyimibahrudin.com',     'Alex',   'Rivera',     '+1-555-1007', '2000-06-18', 'pending',  'standard', 'ALEX00',     NULL, '2026-03-10 16:00:00+00', '2026-03-10 16:30:00+00'),
  ('hasyimi+fatima@hasyimibahrudin.com',   'Fatima', 'Al-Rashid',  '+1-555-1008', '1997-12-05', 'verified', 'premium',  'FATIMA97',   4,   '2026-02-01 12:00:00+00', '2026-03-21 08:00:00+00'),
  ('hasyimi+ben@hasyimibahrudin.com',      'Ben',    'O''Brien',   '+1-555-1009', '1985-08-20', 'rejected', 'standard', NULL,         NULL, '2026-03-15 10:00:00+00', NULL),
  ('hasyimi+chloe@hasyimibahrudin.com',    'Chloe',  'Dubois',     '+1-555-1010', '1996-02-14', 'verified', 'standard', 'CHLOE96',    3,   '2026-03-01 09:00:00+00', '2026-03-17 11:00:00+00');

-- Accounts
INSERT INTO accounts (customer_id, type, currency, balance_cents, status, opened_at) VALUES
  (1, 'checking',   'USD', 1245000,  'active', '2025-06-01 10:00:00+00'),
  (1, 'savings',    'USD', 5200000,  'active', '2025-07-15 08:00:00+00'),
  (1, 'investment', 'USD', 15000000, 'active', '2025-09-01 12:00:00+00'),
  (2, 'checking',   'USD', 340500,   'active', '2025-08-10 14:00:00+00'),
  (2, 'savings',    'USD', 890000,   'active', '2025-10-01 10:00:00+00'),
  (3, 'checking',   'USD', 78200,    'active', '2025-10-01 09:00:00+00'),
  (4, 'checking',   'USD', 560000,   'active', '2025-12-15 11:00:00+00'),
  (4, 'savings',    'USD', 2100000,  'active', '2026-01-01 08:00:00+00'),
  (5, 'checking',   'USD', 125000,   'active', '2026-01-05 08:00:00+00'),
  (6, 'checking',   'USD', 210300,   'active', '2026-01-20 03:00:00+00'),
  (7, 'checking',   'USD', 0,        'active', '2026-03-10 16:00:00+00'),
  (8, 'checking',   'USD', 890000,   'active', '2026-02-01 12:00:00+00'),
  (8, 'savings',    'USD', 4500000,  'active', '2026-02-15 09:00:00+00'),
  (10, 'checking',  'USD', 45000,    'active', '2026-03-01 09:00:00+00');

-- Cards
INSERT INTO cards (customer_id, account_id, type, last_four, status, daily_limit_cents, issued_at) VALUES
  (1, 1,  'physical', '4821', 'active',    1000000, '2025-06-05 10:00:00+00'),
  (1, 1,  'virtual',  '9934', 'active',     500000, '2025-08-01 12:00:00+00'),
  (2, 4,  'physical', '3355', 'active',     500000, '2025-08-15 14:00:00+00'),
  (3, 6,  'virtual',  '7712', 'active',     200000, '2025-10-05 09:00:00+00'),
  (4, 7,  'physical', '6688', 'active',     500000, '2026-01-01 11:00:00+00'),
  (5, 9,  'virtual',  '1147', 'active',     200000, '2026-01-10 08:00:00+00'),
  (6, 10, 'virtual',  '8823', 'frozen',     200000, '2026-01-25 03:00:00+00'),
  (8, 12, 'physical', '5599', 'active',    1000000, '2026-02-05 12:00:00+00');

-- Transactions (recent activity for active customers)
INSERT INTO transactions (account_id, type, amount_cents, currency, merchant_name, category, description, status, created_at) VALUES
  -- Sarah (premium, heavy user)
  (1, 'purchase',    4500, 'USD', 'Whole Foods',        'groceries',      NULL,                          'completed', '2026-03-20 11:00:00+00'),
  (1, 'purchase',    8900, 'USD', 'Uber Eats',          'dining',         NULL,                          'completed', '2026-03-20 19:00:00+00'),
  (1, 'purchase',   15000, 'USD', 'Delta Airlines',     'transport',      'Flight to NYC',               'completed', '2026-03-19 14:00:00+00'),
  (1, 'transfer',  200000, 'USD', NULL,                 'transfer',       'To savings',                  'completed', '2026-03-18 09:00:00+00'),
  (2, 'deposit',   200000, 'USD', NULL,                 'transfer',       'From checking',               'completed', '2026-03-18 09:00:00+00'),
  (1, 'deposit',   500000, 'USD', NULL,                 NULL,             'Payroll - Acme Corp',         'completed', '2026-03-15 06:00:00+00'),
  -- Mike (plus tier)
  (4, 'purchase',    3200, 'USD', 'Starbucks',          'dining',         NULL,                          'completed', '2026-03-20 08:00:00+00'),
  (4, 'purchase',   12500, 'USD', 'Amazon',             'other',          'Electronics',                 'completed', '2026-03-19 20:00:00+00'),
  (4, 'deposit',   350000, 'USD', NULL,                 NULL,             'Payroll - TechCo',            'completed', '2026-03-15 06:00:00+00'),
  (4, 'purchase',    6700, 'USD', 'Netflix',            'entertainment',  'Annual plan',                 'completed', '2026-03-14 12:00:00+00'),
  -- Emma (standard, lower balance)
  (6, 'purchase',    2100, 'USD', 'Target',             'groceries',      NULL,                          'completed', '2026-03-19 16:00:00+00'),
  (6, 'deposit',   150000, 'USD', NULL,                 NULL,             'Payroll - Retail Inc',        'completed', '2026-03-15 06:00:00+00'),
  (6, 'purchase',    4500, 'USD', 'Spotify',            'entertainment',  NULL,                          'completed', '2026-03-10 10:00:00+00'),
  -- Raj (plus tier, active)
  (7, 'purchase',    8800, 'USD', 'Trader Joe''s',      'groceries',      NULL,                          'completed', '2026-03-21 07:00:00+00'),
  (7, 'purchase',   25000, 'USD', 'Airbnb',             'entertainment',  'Weekend getaway',             'completed', '2026-03-20 10:00:00+00'),
  (7, 'transfer',  100000, 'USD', NULL,                 'transfer',       'To savings',                  'completed', '2026-03-19 08:00:00+00'),
  (8, 'deposit',   100000, 'USD', NULL,                 'transfer',       'From checking',               'completed', '2026-03-19 08:00:00+00'),
  (7, 'deposit',   400000, 'USD', NULL,                 NULL,             'Payroll - ConsultCo',         'completed', '2026-03-15 06:00:00+00'),
  -- Fatima (premium)
  (12, 'purchase',   3500, 'USD', 'Blue Bottle Coffee', 'dining',         NULL,                          'completed', '2026-03-21 07:30:00+00'),
  (12, 'purchase',  45000, 'USD', 'Nordstrom',          'other',          NULL,                          'completed', '2026-03-20 15:00:00+00'),
  (12, 'deposit',  800000, 'USD', NULL,                 NULL,             'Payroll - FinanceOrg',        'completed', '2026-03-15 06:00:00+00'),
  (12, 'transfer', 300000, 'USD', NULL,                 'transfer',       'To savings',                  'completed', '2026-03-16 09:00:00+00'),
  (13, 'deposit',  300000, 'USD', NULL,                 'transfer',       'From checking',               'completed', '2026-03-16 09:00:00+00'),
  -- Yuki (card frozen - suspicious activity)
  (10, 'purchase',   9900, 'USD', 'Unknown Merchant',   'other',          'Flagged transaction',         'reversed',  '2026-03-20 03:00:00+00'),
  (10, 'purchase',   2500, 'USD', 'Lawson',             'groceries',      NULL,                          'completed', '2026-03-18 04:00:00+00');

-- Support tickets
INSERT INTO support_tickets (customer_id, subject, category, status, priority, created_at, resolved_at) VALUES
  (6, 'Suspicious charge on my account',       'fraud',   'in_progress', 'urgent',  '2026-03-20 03:30:00+00', NULL),
  (3, 'How to set up direct deposit?',         'account', 'resolved',    'normal',  '2026-03-15 12:00:00+00', '2026-03-15 14:00:00+00'),
  (5, 'Card not working at ATM',               'card',    'resolved',    'high',    '2026-03-12 09:00:00+00', '2026-03-12 11:00:00+00'),
  (10,'When will my card arrive?',             'card',    'open',        'normal',  '2026-03-16 10:00:00+00', NULL),
  (4, 'Incorrect fee charged',                 'billing', 'open',        'normal',  '2026-03-20 14:00:00+00', NULL),
  (1, 'Upgrade investment account limit',      'account', 'resolved',    'low',     '2026-02-20 08:00:00+00', '2026-02-21 10:00:00+00');
