# Distributed E-Commerce Checkout Engine

A simple, single-file Go project that copies how an online store's checkout works — built to demonstrate the **Saga pattern** in a distributed, event-driven system.

## What it does

When someone "buys" something, three steps happen in order:

1. **Reserve inventory** — check the item is in stock, set it aside
2. **Charge payment** — bill the customer
3. **Create shipment** — send the item out

If any step fails partway (e.g. payment gets declined), the system automatically **undoes** the earlier steps — like giving the reserved stock back. This "do it in order, undo it if it breaks" idea is called the **Saga pattern**, and it's the core concept this project demonstrates.

## Why is it all in one file, with nothing to install?

A real system like this normally runs on:
- **Kafka** — sends messages between different services
- **PostgreSQL** — stores orders in a real database
- **Redis** — caches data and manages distributed locks

Setting all of that up takes time. So this project fakes each one with plain Go code, while keeping the actual logic real:

| Real tool | Faked here with |
|---|---|
| Kafka | Go channels (in-memory pub/sub) |
| PostgreSQL | A list stored in memory |
| Redis | Another in-memory list, used for caching + locking |
| gRPC | A plain HTTP endpoint |

The ordering and undo (compensation) logic behaves the same as it would with the real tools — only the underlying infrastructure is simplified so anyone can run it instantly, with no setup.

## How to run it

```bash
go run main.go
```

You'll see:
```
checkout engine listening on :8080
```

## How to test it

In a second terminal:

```bash
# Place an order
curl -X POST localhost:8080/checkout -d '{"user_id":"u1","item":"sku-42","qty":2,"amount":59.99}'

# Check its status (copy the "id" from the response above)
curl localhost:8080/status/<order_id>

# See cache hit rate
curl localhost:8080/stats
```

## What to expect

Each order comes back as either:
- `"status": "COMPLETED"` — everything succeeded
- `"status": "ROLLED_BACK"` — something failed, and the earlier steps were undone automatically

About **1 in 5 orders fail on purpose** (simulated payment decline or out-of-stock), so running the checkout command a few times will show both outcomes — proving the undo logic actually works, not just the happy path.

## Tech concepts demonstrated

- Saga pattern with compensating transactions
- Event-driven architecture (pub/sub between services)
- Distributed locking (prevents duplicate processing of the same order)
- Caching to reduce database load
- Concurrent request handling
