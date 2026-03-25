"""
Simulates realistic activity for a Todo app connected to GrowthPipe.
Generates new signups, todo creation, completions, plan upgrades, and logins.

Usage:
  DATABASE_URL=postgres://... python simulate.py [--interval 5] [--once]
"""

import os
import sys
import time
import random
import argparse
from datetime import datetime, timezone

import psycopg2

DATABASE_URL = os.environ.get("DATABASE_URL", "postgres://localhost:5432/todo_app")

FIRST_NAMES = ["Liam", "Olivia", "Noah", "Ava", "Ethan", "Sophia", "Mason", "Mia", "Lucas", "Harper",
               "Aiden", "Ella", "Caden", "Aria", "Logan", "Riley", "Zara", "Leo", "Nora", "Kai"]
LAST_NAMES = ["Smith", "Park", "Garcia", "Nakamura", "Brown", "Singh", "Müller", "Costa", "Ali", "O'Brien",
              "Kowalski", "Fernandez", "Sato", "Williams", "Kim", "Chen", "Johansson", "Ahmed", "Taylor", "Rossi"]
TIMEZONES = ["America/New_York", "America/Los_Angeles", "America/Chicago", "Europe/London",
             "Europe/Berlin", "Asia/Tokyo", "Asia/Seoul", "Asia/Singapore", "Australia/Sydney", "UTC"]
PLANS = ["free", "free", "free", "pro", "team"]  # weighted toward free
TODO_TITLES = [
    "Review pull request", "Write unit tests", "Update documentation", "Fix login bug",
    "Prepare presentation", "Buy groceries", "Call dentist", "Plan team offsite",
    "Read chapter 3", "Deploy to staging", "Set up CI pipeline", "Design landing page",
    "Write blog post", "Organize bookmarks", "Clean up desktop", "Schedule 1:1",
    "File expense report", "Update dependencies", "Migrate database", "Refactor auth module",
    "Morning workout", "Meal prep for the week", "Send invoice", "Book flight",
    "Research competitors", "Create wireframes", "Ship feature flag", "Review analytics",
]
LABEL_NAMES = ["Work", "Personal", "Urgent", "Health", "Learning", "Finance", "Home"]
LABEL_COLORS = ["#ef4444", "#3b82f6", "#f59e0b", "#10b981", "#8b5cf6", "#ec4899", "#6b7280"]


def connect():
    return psycopg2.connect(DATABASE_URL)


def now():
    return datetime.now(timezone.utc)


def simulate_new_signup(conn):
    """New user signs up."""
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    email = f"hasyimi+{first.lower()}.{last.lower()}.{random.randint(100,999)}@hasyimibahrudin.com"
    plan = random.choice(PLANS)
    tz = random.choice(TIMEZONES)

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO users (email, first_name, last_name, plan, timezone, signed_up_at, last_login_at) VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id",
            (email, first, last, plan, tz, now(), now()),
        )
        user_id = cur.fetchone()[0]

        # New users often create a label and a todo
        if random.random() < 0.6:
            idx = random.randint(0, len(LABEL_NAMES) - 1)
            cur.execute(
                "INSERT INTO labels (user_id, name, color) VALUES (%s, %s, %s)",
                (user_id, LABEL_NAMES[idx], LABEL_COLORS[idx]),
            )

        if random.random() < 0.8:
            cur.execute(
                "INSERT INTO todos (user_id, title, created_at) VALUES (%s, %s, %s)",
                (user_id, random.choice(TODO_TITLES), now()),
            )

    conn.commit()
    print(f"[signup] {first} {last} ({email}) — plan={plan}")


def simulate_new_todo(conn):
    """Existing user creates a todo."""
    with conn.cursor() as cur:
        cur.execute("SELECT id, first_name FROM users ORDER BY random() LIMIT 1")
        row = cur.fetchone()
        if not row:
            return
        user_id, name = row
        title = random.choice(TODO_TITLES)
        due = None
        if random.random() < 0.5:
            from datetime import timedelta
            due = (now() + timedelta(days=random.randint(1, 14))).date()

        cur.execute(
            "INSERT INTO todos (user_id, title, due_date, created_at) VALUES (%s, %s, %s, %s)",
            (user_id, title, due, now()),
        )
        # Update last login
        cur.execute("UPDATE users SET last_login_at = %s WHERE id = %s", (now(), user_id))
    conn.commit()
    print(f"[todo] {name} created: {title}")


def simulate_complete_todo(conn):
    """User completes a todo."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT t.id, t.title, u.id, u.first_name FROM todos t JOIN users u ON u.id = t.user_id "
            "WHERE t.completed = false ORDER BY random() LIMIT 1"
        )
        row = cur.fetchone()
        if not row:
            return
        todo_id, title, user_id, name = row
        cur.execute(
            "UPDATE todos SET completed = true, completed_at = %s WHERE id = %s",
            (now(), todo_id),
        )
        cur.execute("UPDATE users SET last_login_at = %s WHERE id = %s", (now(), user_id))
    conn.commit()
    print(f"[complete] {name} completed: {title}")


def simulate_plan_upgrade(conn):
    """User upgrades their plan."""
    with conn.cursor() as cur:
        cur.execute("SELECT id, first_name, plan FROM users WHERE plan = 'free' ORDER BY random() LIMIT 1")
        row = cur.fetchone()
        if not row:
            return
        user_id, name, _ = row
        new_plan = random.choice(["pro", "team"])
        cur.execute("UPDATE users SET plan = %s, last_login_at = %s WHERE id = %s", (new_plan, now(), user_id))
    conn.commit()
    print(f"[upgrade] {name} upgraded to {new_plan}")


def simulate_login(conn):
    """User logs in (updates last_login_at)."""
    with conn.cursor() as cur:
        cur.execute("SELECT id, first_name FROM users ORDER BY random() LIMIT 1")
        row = cur.fetchone()
        if not row:
            return
        user_id, name = row
        cur.execute("UPDATE users SET last_login_at = %s WHERE id = %s", (now(), user_id))
    conn.commit()
    print(f"[login] {name}")


ACTIONS = [
    (simulate_new_signup, 0.15),
    (simulate_new_todo, 0.30),
    (simulate_complete_todo, 0.25),
    (simulate_plan_upgrade, 0.05),
    (simulate_login, 0.25),
]


def pick_action():
    r = random.random()
    cumulative = 0
    for fn, weight in ACTIONS:
        cumulative += weight
        if r < cumulative:
            return fn
    return ACTIONS[-1][0]


def main():
    parser = argparse.ArgumentParser(description="Simulate Todo app activity")
    parser.add_argument("--interval", type=float, default=5, help="Seconds between events (default: 5)")
    parser.add_argument("--once", action="store_true", help="Run one batch and exit")
    args = parser.parse_args()

    conn = connect()
    print(f"Connected to database. Simulating every {args.interval}s...")

    try:
        while True:
            action = pick_action()
            try:
                action(conn)
            except Exception as e:
                print(f"[error] {e}")
                conn.rollback()

            if args.once:
                break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
