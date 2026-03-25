-- Seed data for Todo App

INSERT INTO users (email, first_name, last_name, plan, timezone, signed_up_at, last_login_at) VALUES
  ('hasyimi+alice@hasyimibahrudin.com',   'Alice',  'Chen',       'pro',  'America/New_York',    '2025-11-01 09:00:00+00', '2026-03-20 14:30:00+00'),
  ('hasyimi+bob@hasyimibahrudin.com',     'Bob',    'Martinez',   'free', 'America/Los_Angeles', '2025-12-15 18:00:00+00', '2026-03-18 10:00:00+00'),
  ('hasyimi+carol@hasyimibahrudin.com',   'Carol',  'Nguyen',     'team', 'Asia/Tokyo',          '2026-01-05 03:00:00+00', '2026-03-21 07:00:00+00'),
  ('hasyimi+dave@hasyimibahrudin.com',    'Dave',   'Thompson',   'free', 'Europe/London',       '2026-02-10 12:00:00+00', '2026-02-28 16:00:00+00'),
  ('hasyimi+eve@hasyimibahrudin.com',     'Eve',    'Johansson',  'pro',  'Europe/Stockholm',    '2026-02-20 08:00:00+00', '2026-03-19 09:15:00+00'),
  ('hasyimi+frank@hasyimibahrudin.com',   'Frank',  'Okafor',     'free', 'Africa/Lagos',        '2026-03-01 11:00:00+00', NULL),
  ('hasyimi+grace@hasyimibahrudin.com',   'Grace',  'Kim',        'team', 'Asia/Seoul',          '2026-03-05 02:00:00+00', '2026-03-20 23:00:00+00'),
  ('hasyimi+hiro@hasyimibahrudin.com',    'Hiro',   'Tanaka',     'free', 'Asia/Tokyo',          '2026-03-10 06:00:00+00', '2026-03-15 04:00:00+00'),
  ('hasyimi+irene@hasyimibahrudin.com',   'Irene',  'Dubois',     'pro',  'Europe/Paris',        '2026-03-12 14:00:00+00', '2026-03-21 08:00:00+00'),
  ('hasyimi+jake@hasyimibahrudin.com',    'Jake',   'Wilson',     'free', 'America/Chicago',     '2026-03-18 20:00:00+00', '2026-03-18 20:30:00+00');

-- Labels
INSERT INTO labels (user_id, name, color) VALUES
  (1, 'Work',     '#ef4444'),
  (1, 'Personal', '#3b82f6'),
  (1, 'Urgent',   '#f59e0b'),
  (3, 'Sprint',   '#8b5cf6'),
  (3, 'Backlog',  '#6b7280'),
  (5, 'Health',   '#10b981'),
  (7, 'Team',     '#ec4899');

-- Todos for Alice (power user, pro)
INSERT INTO todos (user_id, title, completed, due_date, created_at, completed_at) VALUES
  (1, 'Prepare Q1 report',           true,  '2026-03-15', '2026-03-01 09:00:00+00', '2026-03-14 17:00:00+00'),
  (1, 'Review PR #482',              true,  '2026-03-10', '2026-03-08 10:00:00+00', '2026-03-09 11:30:00+00'),
  (1, 'Book dentist appointment',    false, '2026-03-25', '2026-03-18 08:00:00+00', NULL),
  (1, 'Update team wiki',            false, '2026-03-28', '2026-03-20 13:00:00+00', NULL),
  (1, 'Plan weekend trip',           false, NULL,          '2026-03-20 19:00:00+00', NULL);

-- Todos for Bob (light user)
INSERT INTO todos (user_id, title, completed, due_date, created_at, completed_at) VALUES
  (2, 'Buy groceries',               true,  '2026-03-17', '2026-03-16 09:00:00+00', '2026-03-17 12:00:00+00'),
  (2, 'Call mom',                     false, NULL,          '2026-03-18 10:00:00+00', NULL);

-- Todos for Carol (team plan, active)
INSERT INTO todos (user_id, title, completed, due_date, created_at, completed_at) VALUES
  (3, 'Design new onboarding flow',  false, '2026-03-30', '2026-03-10 04:00:00+00', NULL),
  (3, 'Sprint retro notes',          true,  '2026-03-14', '2026-03-13 02:00:00+00', '2026-03-14 06:00:00+00'),
  (3, 'Ship v2.1 release',           false, '2026-04-01', '2026-03-20 01:00:00+00', NULL);

-- Todos for Dave (churned — hasn't logged in recently)
INSERT INTO todos (user_id, title, completed, due_date, created_at, completed_at) VALUES
  (4, 'Try out the app',             false, NULL,          '2026-02-10 12:30:00+00', NULL);

-- Todos for Eve
INSERT INTO todos (user_id, title, completed, due_date, created_at, completed_at) VALUES
  (5, 'Morning run',                 true,  '2026-03-19', '2026-03-19 06:00:00+00', '2026-03-19 07:00:00+00'),
  (5, 'Read chapter 5',              false, '2026-03-22', '2026-03-19 20:00:00+00', NULL);

-- Frank signed up but never used it
-- Hiro tried it briefly
INSERT INTO todos (user_id, title, completed, due_date, created_at, completed_at) VALUES
  (8, 'Test todo creation',          true,  NULL,          '2026-03-10 06:05:00+00', '2026-03-10 06:06:00+00');

-- Grace (team, very active)
INSERT INTO todos (user_id, title, completed, due_date, created_at, completed_at) VALUES
  (7, 'Sync with design team',       true,  '2026-03-18', '2026-03-15 03:00:00+00', '2026-03-18 05:00:00+00'),
  (7, 'Finalize API contracts',      false, '2026-03-24', '2026-03-20 02:00:00+00', NULL),
  (7, 'Write migration guide',       false, '2026-03-26', '2026-03-20 22:00:00+00', NULL);

-- Irene
INSERT INTO todos (user_id, title, completed, due_date, created_at, completed_at) VALUES
  (9, 'Submit expense report',       false, '2026-03-22', '2026-03-21 08:00:00+00', NULL);

-- Todo labels
INSERT INTO todo_labels (todo_id, label_id) VALUES
  (1, 1),  -- Q1 report → Work
  (1, 3),  -- Q1 report → Urgent
  (2, 1),  -- PR review → Work
  (3, 2),  -- Dentist → Personal
  (5, 2),  -- Weekend trip → Personal
  (8, 4),  -- Design onboarding → Sprint
  (10, 4), -- Ship v2.1 → Sprint
  (10, 5), -- Ship v2.1 → Backlog
  (12, 6), -- Morning run → Health
  (15, 7), -- Sync with design → Team
  (16, 7); -- API contracts → Team
