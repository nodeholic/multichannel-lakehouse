"""Generate simulated multi-channel order events and write them to the Bronze layer (JSON Lines).

Each line is ONE EVENT: a full snapshot of an order at a point in time, like the webhook
payload Shopify/Shopee/TikTok Shop send. An order emits several events as its status
changes (PENDING -> PROCESSING -> COMPLETED ...).

Dirty data is injected on purpose so the Silver layer has real work to do:
  - duplicated events (webhook retries)
  - events missing order_id
  - unnormalized emails (upper case, stray whitespace)
  - malformed timestamps, unknown statuses, empty item lists, invalid item quantities

Run:  python src/data_generator.py --start-date 2026-11-01 --days 7 --orders-per-day 200
"""
import argparse
import json
import random
import uuid
from datetime import date, datetime, timedelta

from faker import Faker

from config import BRONZE_ORDERS_DIR

CHANNELS = ["SHOPIFY", "SHOPEE", "TIKTOK_SHOP", "LAZADA"]
TIERS = ["Standard", "Silver", "Gold", "Platinum"]

# Fixed product catalog -> becomes dim_product later. Prices in VND.
PRODUCTS = [
    {"product_id": f"P{i:03d}", "product_name": name, "category": cat, "list_price": price}
    for i, (name, cat, price) in enumerate(
        [
            ("Bluetooth Headphones", "Electronics", 590000),
            ("20000mAh Power Bank", "Electronics", 450000),
            ("Mechanical Keyboard", "Electronics", 1290000),
            ("Cotton T-Shirt", "Fashion", 199000),
            ("Jeans", "Fashion", 459000),
            ("Running Shoes", "Fashion", 1150000),
            ("Air Fryer", "Home", 1890000),
            ("Blender", "Home", 790000),
            ("Insulated Bottle", "Home", 250000),
            ("Facial Cleanser", "Beauty", 165000),
            ("Sunscreen", "Beauty", 320000),
            ("Lipstick", "Beauty", 280000),
        ],
        start=1,
    )
]

# Probability of each terminal status (a tiny state machine).
FINAL_STATUS_WEIGHTS = {"COMPLETED": 0.80, "CANCELLED": 0.12, "REFUNDED": 0.08}

# Per-event probability of each kind of malformed payload a real source sends occasionally.
CORRUPTION_RATES = {
    "invalid_timestamp": 0.004,
    "unknown_status": 0.005,
    "empty_items": 0.003,
    "invalid_item": 0.004,
}


def corrupt_event(event: dict) -> dict:
    """Return a copy of the event with at most one kind of corruption applied."""
    event = dict(event)
    roll = random.random()
    for kind, rate in CORRUPTION_RATES.items():
        if roll < rate:
            if kind == "invalid_timestamp":
                event["created_at"] = "2026-13-45T99:99:99"  # month 13, hour 99
            elif kind == "unknown_status":
                event["order_status"] = "SHIPPED"  # a status the pipeline does not know yet
            elif kind == "empty_items":
                event["items"] = []
            elif kind == "invalid_item":
                event["items"] = [dict(event["items"][0], quantity=-1)] + event["items"][1:]
            return event
        roll -= rate
    return event


def build_customer_pool(fake: Faker, size: int, start: date, days: int) -> list[dict]:
    """Create a fixed set of customers. Some customers UPGRADE their membership tier on a
    given day within the simulated period -> raw material for SCD Type 2 in dim_customer."""
    pool = []
    for i in range(size):
        base_tier = random.choices(TIERS[:3], weights=[0.6, 0.3, 0.1])[0]
        upgrade_day = start + timedelta(days=random.randint(1, max(days - 1, 1)))
        pool.append(
            {
                "customer_id": f"C{i:05d}",
                "name": fake.name(),
                "email": fake.unique.email(),
                "phone": fake.numerify("09########"),
                "city": fake.city(),
                "base_tier": base_tier,
                # 25% of customers move up one tier on upgrade_day
                "upgrade_day": upgrade_day if random.random() < 0.25 else None,
            }
        )
    return pool


def tier_on(customer: dict, day: date) -> str:
    """Membership tier of a customer on a given day."""
    tier = customer["base_tier"]
    if customer["upgrade_day"] and day >= customer["upgrade_day"]:
        tier = TIERS[min(TIERS.index(tier) + 1, len(TIERS) - 1)]
    return tier


def make_customer_payload(customer: dict, day: date) -> dict:
    email = customer["email"]
    if random.random() < 0.05:  # dirty data: upper-cased email with stray whitespace
        email = f"  {email.upper()} "
    return {
        "customer_id": customer["customer_id"],
        "name": customer["name"],
        "email": email,
        "phone": customer["phone"],
        "city": customer["city"],
        "membership_tier": tier_on(customer, day),
    }


def make_items() -> list[dict]:
    items = []
    for product in random.sample(PRODUCTS, k=random.randint(1, 3)):
        qty = random.randint(1, 3)
        gross = product["list_price"] * qty
        discount = round(gross * random.choice([0, 0, 0.05, 0.1, 0.2]), 2)
        items.append(
            {
                "product_id": product["product_id"],
                "product_name": product["product_name"],
                "category": product["category"],
                "quantity": qty,
                "unit_price": product["list_price"],
                "discount_amount": discount,
            }
        )
    return items


def order_lifecycle(created_at: datetime) -> list[tuple[str, datetime]]:
    """The (status, timestamp) steps an order goes through."""
    steps = [("PENDING", created_at)]
    t = created_at + timedelta(minutes=random.randint(5, 120))
    steps.append(("PROCESSING", t))
    final = random.choices(list(FINAL_STATUS_WEIGHTS), weights=FINAL_STATUS_WEIGHTS.values())[0]
    if final == "REFUNDED":  # a refund only happens after the order was completed
        t += timedelta(hours=random.randint(6, 48))
        steps.append(("COMPLETED", t))
    t += timedelta(hours=random.randint(1, 72))
    steps.append((final, t))
    return steps


def generate_events(pool: list[dict], start: date, days: int, orders_per_day: int) -> list[dict]:
    events = []
    order_seq = 0
    for d in range(days):
        day = start + timedelta(days=d)
        for _ in range(orders_per_day):
            order_seq += 1
            customer = random.choice(pool)
            created_at = datetime.combine(day, datetime.min.time()) + timedelta(
                seconds=random.randint(0, 86399)
            )
            order_id = f"ORD-{order_seq:07d}"
            channel = random.choice(CHANNELS)
            items = make_items()
            customer_payload = make_customer_payload(customer, day)

            for status, updated_at in order_lifecycle(created_at):
                event = {
                    "event_id": str(uuid.uuid4()),
                    "order_id": order_id,
                    "channel": channel,
                    "order_status": status,
                    "created_at": created_at.isoformat(),
                    "updated_at": updated_at.isoformat(),
                    "customer": customer_payload,
                    "items": items,
                    "currency": "VND",
                }
                if random.random() < 0.01:  # dirty data: missing order_id
                    event["order_id"] = None
                event = corrupt_event(event)
                events.append(event)
                if random.random() < 0.03:  # dirty data: webhook delivered twice
                    events.append(dict(event, event_id=str(uuid.uuid4())))
    return events


def write_bronze(events: list[dict]) -> dict[str, int]:
    """Write events partitioned by the day they ARRIVED (updated_at), Hive-style:
    bronze/orders/year=YYYY/month=MM/day=DD/events-xxxx.jsonl
    Bronze is append-only: payloads are never modified."""
    by_day: dict[date, list[dict]] = {}
    for e in events:
        by_day.setdefault(datetime.fromisoformat(e["updated_at"]).date(), []).append(e)

    stats = {}
    for day, day_events in sorted(by_day.items()):
        folder = BRONZE_ORDERS_DIR / f"year={day:%Y}" / f"month={day:%m}" / f"day={day:%d}"
        folder.mkdir(parents=True, exist_ok=True)
        # Each run writes a new file (never overwrites) -> simulates data arriving over time
        path = folder / f"events-{uuid.uuid4().hex[:8]}.jsonl"
        with path.open("w", encoding="utf-8") as f:
            for e in day_events:
                f.write(json.dumps(e, ensure_ascii=False) + "\n")
        stats[str(day)] = len(day_events)
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate simulated orders into the Bronze layer")
    parser.add_argument("--start-date", type=date.fromisoformat, default=date(2026, 11, 1))
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--orders-per-day", type=int, default=200)
    parser.add_argument("--customers", type=int, default=300)
    parser.add_argument("--seed", type=int, default=42, help="Same seed -> same data")
    args = parser.parse_args()

    random.seed(args.seed)
    fake = Faker("vi_VN")  # Vietnamese names and cities: the channels sell in Vietnam
    Faker.seed(args.seed)

    pool = build_customer_pool(fake, args.customers, args.start_date, args.days)
    events = generate_events(pool, args.start_date, args.days, args.orders_per_day)
    stats = write_bronze(events)

    print(f"Wrote {len(events)} events to {BRONZE_ORDERS_DIR}")
    for day, n in stats.items():
        print(f"  {day}: {n} events")


if __name__ == "__main__":
    main()
