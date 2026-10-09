"""A deterministic stream of e-commerce orders.

A generator, not a list: the pipeline pulls one order at a time, so a batch of
a million orders costs the same memory as a batch of ten.
"""

from __future__ import annotations

import random
from collections.abc import Iterator

FIRST = ["Ada", "Grace", "Linus", "Guido", "Margaret", "Ken", "Barbara", "Dennis",
         "Frances", "Edsger", "Radia", "Tim", "Katherine", "Alan", "Hedy", "Donald"]
LAST = ["Lovelace", "Hopper", "Torvalds", "van Rossum", "Hamilton", "Thompson",
        "Liskov", "Ritchie", "Allen", "Dijkstra", "Perlman", "Berners-Lee",
        "Johnson", "Turing", "Lamarr", "Knuth"]
CITIES = ["Utrecht", "Amsterdam", "Rotterdam", "Eindhoven", "Groningen", "Leiden",
          "Delft", "Nijmegen", "Haarlem", "Maastricht"]
PRODUCTS = [
    ("MUG-PYCON", "PyCon NL mug", 14.50),
    ("TEE-SNAKE", "Snake t-shirt", 24.00),
    ("STICKER-PACK", "Sticker pack", 6.00),
    ("HOODIE-ASYNC", "async hoodie", 49.00),
    ("BOOK-FLUENT", "Fluent Python", 59.90),
    ("KEYCAP-PY", "Python keycap", 9.75),
]


def order_id(n: int) -> str:
    return f"order-{n:04d}"


def make_order(n: int, seed: int = 2026) -> dict:
    rng = random.Random(f"{seed}:{n}")
    first, last = rng.choice(FIRST), rng.choice(LAST)
    sku, title, price = rng.choice(PRODUCTS)
    qty = rng.choice([1, 1, 1, 2, 3])
    return {
        "id": order_id(n),
        "customer": f"{first} {last}",
        "email": f"{first}.{last.replace(' ', '')}@example.com".lower(),
        "city": rng.choice(CITIES),
        "sku": sku,
        "title": title,
        "qty": qty,
        "amount": round(price * qty, 2),
    }


def lookup(order: str, seed: int = 2026) -> dict:
    return make_order(int(order.rsplit("-", 1)[1]), seed)


def iter_orders(count: int, seed: int = 2026) -> Iterator[dict]:
    for n in range(1, count + 1):
        yield make_order(n, seed)
