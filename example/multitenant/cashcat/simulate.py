"""
Simulates realistic activity for CashCat neobank connected to GrowthPipe.
Generates signups, transactions, card events, support tickets, and tier upgrades.

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

DATABASE_URL = os.environ.get("DATABASE_URL", "postgres://localhost:5432/cashcat")

FIRST_NAMES = ["Liam", "Olivia", "Noah", "Ava", "Ethan", "Sophia", "Mason", "Mia", "Lucas", "Harper",
               "Aiden", "Ella", "Caden", "Aria", "Logan", "Riley", "Zara", "Leo", "Nora", "Kai"]
LAST_NAMES = ["Smith", "Park", "Garcia", "Nakamura", "Brown", "Singh", "Müller", "Costa", "Ali", "O'Brien",
              "Kowalski", "Fernandez", "Sato", "Williams", "Kim", "Chen", "Johansson", "Ahmed", "Taylor", "Rossi"]
TIERS = ["standard", "standard", "standard", "plus", "premium"]

MERCHANTS = [
    ("Whole Foods", "groceries"), ("Trader Joe's", "groceries"), ("Target", "groceries"),
    ("Starbucks", "dining"), ("Chipotle", "dining"), ("Uber Eats", "dining"), ("Blue Bottle Coffee", "dining"),
    ("Uber", "transport"), ("Lyft", "transport"), ("Delta Airlines", "transport"),
    ("Netflix", "entertainment"), ("Spotify", "entertainment"), ("Steam", "entertainment"),
    ("Con Edison", "utilities"), ("Verizon", "utilities"), ("Comcast", "utilities"),
    ("Amazon", "other"), ("Nordstrom", "other"), ("Apple Store", "other"), ("IKEA", "other"),
]

TICKET_SUBJECTS = [
    ("How to set up direct deposit?", "account", "normal"),
    ("Card not working at ATM", "card", "high"),
    ("Suspicious charge on my account", "fraud", "urgent"),
    ("When will my card arrive?", "card", "normal"),
    ("Incorrect fee charged", "billing", "normal"),
    ("Can I increase my daily limit?", "card", "low"),
    ("How to close my account?", "account", "normal"),
    ("Refund not received", "billing", "high"),
    ("Lost my physical card", "card", "high"),
    ("Two-factor auth not working", "account", "urgent"),
]


def connect():
    return psycopg2.connect(DATABASE_URL)


def now():
    return datetime.now(timezone.utc)


def simulate_new_customer(conn):
    """New customer signs up with checking account."""
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    email = f"hasyimi+{first.lower()}.{last.lower()}.{random.randint(100,999)}@hasyimibahrudin.com"
    tier = random.choice(TIERS)
    referral_code = f"{first.upper()}{random.randint(100,999)}"

    # Maybe referred by existing customer
    referred_by = None
    with conn.cursor() as cur:
        if random.random() < 0.3:
            cur.execute("SELECT id FROM customers WHERE referral_code IS NOT NULL ORDER BY random() LIMIT 1")
            row = cur.fetchone()
            if row:
                referred_by = row[0]

        cur.execute(
            "INSERT INTO customers (email, first_name, last_name, phone, date_of_birth, kyc_status, tier, referral_code, referred_by, signed_up_at, last_active_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id",
            (email, first, last, f"+1-555-{random.randint(1000,9999)}",
             f"{random.randint(1980,2002)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
             random.choice(["pending", "verified", "verified", "verified"]),
             tier, referral_code, referred_by, now(), now()),
        )
        customer_id = cur.fetchone()[0]

        # Open checking account with initial deposit
        deposit = random.randint(10000, 500000)  # $100-$5000
        cur.execute(
            "INSERT INTO accounts (customer_id, type, balance_cents, opened_at) VALUES (%s, 'checking', %s, %s) RETURNING id",
            (customer_id, deposit, now()),
        )
        account_id = cur.fetchone()[0]

        cur.execute(
            "INSERT INTO transactions (account_id, type, amount_cents, description, created_at) VALUES (%s, 'deposit', %s, 'Initial deposit', %s)",
            (account_id, deposit, now()),
        )

        # Issue virtual card (70% chance)
        if random.random() < 0.7:
            last_four = f"{random.randint(1000,9999)}"
            cur.execute(
                "INSERT INTO cards (customer_id, account_id, type, last_four, issued_at) VALUES (%s, %s, 'virtual', %s, %s)",
                (customer_id, account_id, last_four, now()),
            )

    conn.commit()
    ref_text = " (referred)" if referred_by else ""
    print(f"[signup] {first} {last} ({email}) — tier={tier}, deposit=${deposit/100:.2f}{ref_text}")


def simulate_purchase(conn):
    """Customer makes a purchase."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT a.id, a.balance_cents, c.id, c.first_name, c.last_name FROM accounts a "
            "JOIN customers c ON c.id = a.customer_id "
            "WHERE a.status = 'active' AND a.balance_cents > 500 "
            "ORDER BY random() LIMIT 1"
        )
        row = cur.fetchone()
        if not row:
            return
        account_id, balance, customer_id, first_name, last_name = row
        name = f"{first_name} {last_name or ''}".strip()

        merchant, category = random.choice(MERCHANTS)
        amount = random.randint(200, min(balance, 50000))  # $2-$500

        cur.execute(
            "INSERT INTO transactions (account_id, type, amount_cents, merchant_name, category, status, created_at) "
            "VALUES (%s, 'purchase', %s, %s, %s, 'completed', %s)",
            (account_id, amount, merchant, category, now()),
        )
        cur.execute("UPDATE accounts SET balance_cents = balance_cents - %s WHERE id = %s", (amount, account_id))
        cur.execute("UPDATE customers SET last_active_at = %s WHERE id = %s", (now(), customer_id))
    conn.commit()
    print(f"[purchase] {name} spent ${amount/100:.2f} at {merchant}")


def simulate_deposit(conn):
    """Payroll or manual deposit."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT a.id, c.id, c.first_name FROM accounts a "
            "JOIN customers c ON c.id = a.customer_id "
            "WHERE a.status = 'active' AND a.type = 'checking' "
            "ORDER BY random() LIMIT 1"
        )
        row = cur.fetchone()
        if not row:
            return
        account_id, customer_id, name = row

        amount = random.choice([150000, 200000, 250000, 350000, 400000, 500000, 800000])
        desc = random.choice(["Payroll", "Payroll", "Payroll", "Transfer from external", "Freelance payment"])

        cur.execute(
            "INSERT INTO transactions (account_id, type, amount_cents, description, created_at) VALUES (%s, 'deposit', %s, %s, %s)",
            (account_id, amount, desc, now()),
        )
        cur.execute("UPDATE accounts SET balance_cents = balance_cents + %s WHERE id = %s", (amount, account_id))
        cur.execute("UPDATE customers SET last_active_at = %s WHERE id = %s", (now(), customer_id))
    conn.commit()
    print(f"[deposit] {name} received ${amount/100:.2f} — {desc}")


def simulate_transfer(conn):
    """Transfer between checking and savings."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT c.id, c.first_name FROM customers c "
            "JOIN accounts a ON a.customer_id = c.id "
            "GROUP BY c.id, c.first_name HAVING count(*) >= 2 "
            "ORDER BY random() LIMIT 1"
        )
        row = cur.fetchone()
        if not row:
            return
        customer_id, name = row

        cur.execute(
            "SELECT id, type, balance_cents FROM accounts WHERE customer_id = %s AND status = 'active' ORDER BY type",
            (customer_id,),
        )
        accounts = cur.fetchall()
        if len(accounts) < 2:
            return

        from_acc = accounts[0]  # checking
        to_acc = accounts[1]    # savings
        if from_acc[2] < 10000:
            return

        amount = random.randint(10000, min(from_acc[2] // 2, 500000))

        cur.execute(
            "INSERT INTO transactions (account_id, type, amount_cents, category, description, created_at) VALUES (%s, 'transfer', %s, 'transfer', 'To savings', %s)",
            (from_acc[0], amount, now()),
        )
        cur.execute(
            "INSERT INTO transactions (account_id, type, amount_cents, category, description, created_at) VALUES (%s, 'deposit', %s, 'transfer', 'From checking', %s)",
            (to_acc[0], amount, now()),
        )
        cur.execute("UPDATE accounts SET balance_cents = balance_cents - %s WHERE id = %s", (amount, from_acc[0]))
        cur.execute("UPDATE accounts SET balance_cents = balance_cents + %s WHERE id = %s", (amount, to_acc[0]))
        cur.execute("UPDATE customers SET last_active_at = %s WHERE id = %s", (now(), customer_id))
    conn.commit()
    print(f"[transfer] {name} moved ${amount/100:.2f} to savings")


def simulate_open_savings(conn):
    """Customer opens a savings account."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT c.id, c.first_name FROM customers c "
            "LEFT JOIN accounts a ON a.customer_id = c.id AND a.type = 'savings' "
            "WHERE a.id IS NULL AND c.kyc_status = 'verified' "
            "ORDER BY random() LIMIT 1"
        )
        row = cur.fetchone()
        if not row:
            return
        customer_id, name = row
        cur.execute(
            "INSERT INTO accounts (customer_id, type, balance_cents, opened_at) VALUES (%s, 'savings', 0, %s)",
            (customer_id, now()),
        )
        cur.execute("UPDATE customers SET last_active_at = %s WHERE id = %s", (now(), customer_id))
    conn.commit()
    print(f"[account] {name} opened a savings account")


def simulate_support_ticket(conn):
    """Customer opens a support ticket."""
    with conn.cursor() as cur:
        cur.execute("SELECT id, first_name FROM customers ORDER BY random() LIMIT 1")
        row = cur.fetchone()
        if not row:
            return
        customer_id, name = row
        subject, category, priority = random.choice(TICKET_SUBJECTS)
        cur.execute(
            "INSERT INTO support_tickets (customer_id, subject, category, priority, created_at) VALUES (%s, %s, %s, %s, %s)",
            (customer_id, subject, category, priority, now()),
        )
        cur.execute("UPDATE customers SET last_active_at = %s WHERE id = %s", (now(), customer_id))
    conn.commit()
    print(f"[ticket] {name}: {subject} ({priority})")


def simulate_resolve_ticket(conn):
    """Resolve an open support ticket."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT t.id, c.first_name, t.subject FROM support_tickets t "
            "JOIN customers c ON c.id = t.customer_id "
            "WHERE t.status IN ('open', 'in_progress') ORDER BY random() LIMIT 1"
        )
        row = cur.fetchone()
        if not row:
            return
        ticket_id, name, subject = row
        cur.execute(
            "UPDATE support_tickets SET status = 'resolved', resolved_at = %s WHERE id = %s",
            (now(), ticket_id),
        )
    conn.commit()
    print(f"[resolved] {name}'s ticket: {subject}")


def simulate_tier_upgrade(conn):
    """Customer upgrades tier."""
    with conn.cursor() as cur:
        cur.execute("SELECT id, first_name, tier FROM customers WHERE tier != 'premium' ORDER BY random() LIMIT 1")
        row = cur.fetchone()
        if not row:
            return
        customer_id, name, current_tier = row
        new_tier = "plus" if current_tier == "standard" else "premium"
        cur.execute("UPDATE customers SET tier = %s, last_active_at = %s WHERE id = %s", (new_tier, now(), customer_id))
    conn.commit()
    print(f"[upgrade] {name}: {current_tier} → {new_tier}")


def simulate_freeze_card(conn):
    """Customer freezes or unfreezes a card."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT ca.id, c.first_name, ca.last_four, ca.status FROM cards ca "
            "JOIN customers c ON c.id = ca.customer_id "
            "WHERE ca.status IN ('active', 'frozen') ORDER BY random() LIMIT 1"
        )
        row = cur.fetchone()
        if not row:
            return
        card_id, name, last_four, status = row
        new_status = "frozen" if status == "active" else "active"
        cur.execute("UPDATE cards SET status = %s WHERE id = %s", (new_status, card_id))
    conn.commit()
    action = "froze" if new_status == "frozen" else "unfroze"
    print(f"[card] {name} {action} card •••{last_four}")


ACTIONS = [
    (simulate_new_customer, 0.10),
    (simulate_purchase, 0.30),
    (simulate_deposit, 0.12),
    (simulate_transfer, 0.08),
    (simulate_open_savings, 0.05),
    (simulate_support_ticket, 0.08),
    (simulate_resolve_ticket, 0.07),
    (simulate_tier_upgrade, 0.05),
    (simulate_freeze_card, 0.05),
    # remaining 0.10 = login (update last_active_at)
]


def simulate_login(conn):
    """Customer logs in."""
    with conn.cursor() as cur:
        cur.execute("SELECT id, first_name FROM customers ORDER BY random() LIMIT 1")
        row = cur.fetchone()
        if not row:
            return
        cur.execute("UPDATE customers SET last_active_at = %s WHERE id = %s", (now(), row[0]))
    conn.commit()
    print(f"[login] {row[1]}")


ACTIONS.append((simulate_login, 0.10))


def pick_action():
    r = random.random()
    cumulative = 0
    for fn, weight in ACTIONS:
        cumulative += weight
        if r < cumulative:
            return fn
    return ACTIONS[-1][0]


def main():
    parser = argparse.ArgumentParser(description="Simulate CashCat neobank activity")
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
