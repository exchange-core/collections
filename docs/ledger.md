# Balances

[exchange collections](../README.md) › Balances

`collections-ledger` contains `BalanceTable`: balances — an available and a reserved amount — under keys of a
fixed number of longs, (account, currency) or (user UUID, token, subaccount), in a `LongTupleHashtable`. Neither
amount ever goes below 0 and no sum overflows: an operation checks before it writes and returns a status — a
refusal is not an exception, and nothing allocates.

```java
import exchange.core2.collections.ledger.BalanceTable;

final BalanceTable balances = BalanceTable.builder()
        .keyLongs(2)                // (account, currency)
        .expectedSize(1_000_000)
        .build();

balances.deposit(balances.getOrCreate(buyer, USD), 1_000);        // OK

// an order holds what it may spend
balances.reserve(balances.find(buyer, USD), 500);                 // OK
int status = balances.reserve(balances.find(buyer, USD), 600);    // INSUFFICIENT_AVAILABLE, nothing changes
BalanceTable.statusName(status);                                  // "INSUFFICIENT_AVAILABLE"

// a trade: all the postings or none
final BalanceTable.Batch trade = balances.newBatch(8);  // made once, cleared and reused
trade.add(buyer, USD, 0, -240)       // (key, available delta, reserved delta): the buyer's hold spent
        .add(buyer, BTC, 1, 0)       // the base to the buyer - a new balance
        .add(seller, BTC, 0, -1)
        .add(seller, USD, 236, 0)    // the quote to the seller, minus the fee
        .add(FEES, USD, 4, 0);
balances.apply(trade);                 // OK, or the status of the posting refused - trade.failedPosting()
trade.clear();

// the rest of the order cancelled
balances.release(balances.find(buyer, USD), 260);
```

**References.** `find` and `getOrCreate` return a `long` reference to the balance, and the operations take it — one
probe, then the slot. A reference is valid until the entries move: a resize, a removal, `clear`. One used after
that throws `IllegalArgumentException` instead of reaching another balance; take it right before the operations.
In a table that never resizes (`expectedSize` enough, or `fixedCapacity()`) only a removal makes it stale.
`NO_REF` — `find` of a missing key — is a zero balance.

| Operation | available | reserved | refused with |
|---|---|---|---|
| `deposit(ref, x)` | + x | | `OVERFLOW`; `NO_BALANCE` for `NO_REF` (`getOrCreate` it) |
| `withdraw(ref, x)` | − x | | `INSUFFICIENT_AVAILABLE` |
| `reserve(ref, x)` | − x | + x | `INSUFFICIENT_AVAILABLE`, `OVERFLOW` |
| `release(ref, x)` | + x | − x | `INSUFFICIENT_RESERVED`, `OVERFLOW` |
| `settle(ref, x)` | | − x | `INSUFFICIENT_RESERVED` |
| `post(ref, dA, dR)` | + dA | + dR | any of these |
| `remove(ref)` | | | `NOT_ZERO`, `NO_BALANCE` |

A negative `x` is refused with `INVALID_AMOUNT`. Every refusal leaves the balance as it was.

**Batches.** `apply(batch)` takes the postings in their order, the same key any number of times; the first one
refused undoes those before it, the balances they created included. A posting to a missing key creates its balance
if it takes nothing (no negative delta). A new key that does not fit a full table: `IllegalStateException`, nothing
changed. New balances may resize the table — the references taken before are stale then.

**Notes**
- a probe for `find` / `getOrCreate` and for each posting of a batch; the undo of a refused batch probes again
- keys of 1 to 4 longs have methods of their own, any length goes through a `long[]`
- a balance takes a slot of `8 * (3 + keyLongs)` bytes; the table grows as `LongTupleHashtable` does
- not thread-safe
