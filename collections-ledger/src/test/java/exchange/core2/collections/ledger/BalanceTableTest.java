package exchange.core2.collections.ledger;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static exchange.core2.collections.ledger.BalanceTable.INSUFFICIENT_AVAILABLE;
import static exchange.core2.collections.ledger.BalanceTable.INSUFFICIENT_RESERVED;
import static exchange.core2.collections.ledger.BalanceTable.INVALID_AMOUNT;
import static exchange.core2.collections.ledger.BalanceTable.NOT_ZERO;
import static exchange.core2.collections.ledger.BalanceTable.NO_BALANCE;
import static exchange.core2.collections.ledger.BalanceTable.NO_REF;
import static exchange.core2.collections.ledger.BalanceTable.OK;
import static exchange.core2.collections.ledger.BalanceTable.OVERFLOW;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class BalanceTableTest {

    private static final long USD = 840;
    private static final long BTC = 1000;

    private static BalanceTable table(int keyLongs) {
        return BalanceTable.builder().keyLongs(keyLongs).build();
    }

    private static void assertBalance(BalanceTable t, long ref, long available, long reserved) {
        assertEquals("available", available, t.available(ref));
        assertEquals("reserved", reserved, t.reserved(ref));
    }

    private static void assertStatus(int expected, int actual) {
        assertEquals(BalanceTable.statusName(expected), BalanceTable.statusName(actual));
    }

    @Test
    public void depositCreatesNothingButAddsToAnExistingBalance() {
        final BalanceTable t = table(2);
        final long ref = t.getOrCreate(1L, USD);
        assertBalance(t, ref, 0, 0);
        assertStatus(OK, t.deposit(ref, 1000));
        assertBalance(t, ref, 1000, 0);
        assertEquals(ref, t.find(1L, USD));
        assertEquals(1L, t.key(ref, 0));
        assertEquals(USD, t.key(ref, 1));
        assertEquals(1, t.size());
        t.integrityCheck();
    }

    @Test
    public void aMissingBalanceIsZero() {
        final BalanceTable t = table(2);
        final long none = t.find(1L, USD);
        assertEquals(NO_REF, none);
        assertBalance(t, none, 0, 0);
        assertStatus(INSUFFICIENT_AVAILABLE, t.reserve(none, 1));
        assertStatus(INSUFFICIENT_AVAILABLE, t.withdraw(none, 1));
        assertStatus(INSUFFICIENT_RESERVED, t.release(none, 1));
        assertStatus(INSUFFICIENT_RESERVED, t.settle(none, 1));
        assertStatus(OK, t.reserve(none, 0));
        assertStatus(OK, t.post(none, 0, 0));
        assertStatus(NO_BALANCE, t.deposit(none, 5));
        assertStatus(NO_BALANCE, t.post(none, 0, 5));
        assertStatus(NO_BALANCE, t.remove(none));
        assertThrows(IllegalArgumentException.class, () -> t.key(none, 0));
        assertEquals(0, t.size());
    }

    @Test
    public void reserveReleaseSettleWithdraw() {
        final BalanceTable t = table(2);
        final long ref = t.getOrCreate(1L, USD);
        t.deposit(ref, 1000);
        assertStatus(OK, t.reserve(ref, 600));
        assertBalance(t, ref, 400, 600);
        assertStatus(INSUFFICIENT_AVAILABLE, t.reserve(ref, 401));
        assertBalance(t, ref, 400, 600);
        assertStatus(OK, t.release(ref, 100));
        assertBalance(t, ref, 500, 500);
        assertStatus(OK, t.settle(ref, 400));
        assertBalance(t, ref, 500, 100);
        assertStatus(INSUFFICIENT_RESERVED, t.settle(ref, 101));
        assertStatus(INSUFFICIENT_RESERVED, t.release(ref, 101));
        assertBalance(t, ref, 500, 100);
        assertStatus(INSUFFICIENT_AVAILABLE, t.withdraw(ref, 501));
        assertStatus(OK, t.withdraw(ref, 500));
        assertBalance(t, ref, 0, 100);
        assertStatus(OK, t.post(ref, 50, -50));
        assertBalance(t, ref, 50, 50);
        assertStatus(INSUFFICIENT_RESERVED, t.post(ref, 100, -51));
        assertBalance(t, ref, 50, 50);
        t.integrityCheck();
    }

    @Test
    public void negativeAmountsAreRefused() {
        final BalanceTable t = table(1);
        final long ref = t.getOrCreate(7L);
        t.deposit(ref, 100);
        for (long amount : new long[]{-1, Long.MIN_VALUE}) {
            assertStatus(INVALID_AMOUNT, t.deposit(ref, amount));
            assertStatus(INVALID_AMOUNT, t.withdraw(ref, amount));
            assertStatus(INVALID_AMOUNT, t.reserve(ref, amount));
            assertStatus(INVALID_AMOUNT, t.release(ref, amount));
            assertStatus(INVALID_AMOUNT, t.settle(ref, amount));
        }
        assertBalance(t, ref, 100, 0);
    }

    @Test
    public void overflowIsRefusedAndChangesNothing() {
        final BalanceTable t = table(1);
        final long ref = t.getOrCreate(7L);
        assertStatus(OK, t.deposit(ref, Long.MAX_VALUE));
        assertStatus(OVERFLOW, t.deposit(ref, 1));
        assertBalance(t, ref, Long.MAX_VALUE, 0);
        // reserved at its maximum, then a reservation
        assertStatus(OK, t.reserve(ref, Long.MAX_VALUE));
        assertStatus(OK, t.deposit(ref, 1));
        assertStatus(OVERFLOW, t.reserve(ref, 1));
        assertBalance(t, ref, 1, Long.MAX_VALUE);
        // available at its maximum, then a release
        assertStatus(OK, t.deposit(ref, Long.MAX_VALUE - 1));
        assertStatus(OVERFLOW, t.release(ref, 1));
        assertBalance(t, ref, Long.MAX_VALUE, Long.MAX_VALUE);
        assertStatus(OVERFLOW, t.post(ref, 0, 1));
        assertBalance(t, ref, Long.MAX_VALUE, Long.MAX_VALUE);
        // amounts are never below 0, so a negative delta can not overflow: it is a shortage
        assertStatus(INSUFFICIENT_AVAILABLE, t.post(t.getOrCreate(8L), Long.MIN_VALUE, 0));
        t.integrityCheck();
    }

    @Test
    public void everyKeyLength() {
        for (int keyLongs = 1; keyLongs <= 5; keyLongs++) {
            final BalanceTable t = table(keyLongs);
            final long[] key = new long[keyLongs];
            for (int i = 0; i < keyLongs; i++) {
                key[i] = 10 + i;
            }
            final long ref = switch (keyLongs) {
                case 1 -> t.getOrCreate(key[0]);
                case 2 -> t.getOrCreate(key[0], key[1]);
                case 3 -> t.getOrCreate(key[0], key[1], key[2]);
                case 4 -> t.getOrCreate(key[0], key[1], key[2], key[3]);
                default -> t.getOrCreate(key, 0);
            };
            t.deposit(ref, 5);
            assertEquals(ref, t.find(key, 0));
            final long byArity = switch (keyLongs) {
                case 1 -> t.find(key[0]);
                case 2 -> t.find(key[0], key[1]);
                case 3 -> t.find(key[0], key[1], key[2]);
                case 4 -> t.find(key[0], key[1], key[2], key[3]);
                default -> t.find(key, 0);
            };
            assertEquals(ref, byArity);
            for (int i = 0; i < keyLongs; i++) {
                assertEquals(key[i], t.key(ref, i));
            }
            final BalanceTable.Batch batch = t.newBatch(1);
            switch (keyLongs) {
                case 1 -> batch.add(key[0], 1, 0);
                case 2 -> batch.add(key[0], key[1], 1, 0);
                case 3 -> batch.add(key[0], key[1], key[2], 1, 0);
                case 4 -> batch.add(key[0], key[1], key[2], key[3], 1, 0);
                default -> batch.add(key, 0, 1, 0);
            }
            assertStatus(OK, t.apply(batch));
            assertBalance(t, t.find(key, 0), 6, 0);
        }
    }

    @Test
    public void aReferenceIsStaleAfterAResize() {
        final BalanceTable t = BalanceTable.builder().keyLongs(2).expectedSize(16).build();
        final long first = t.getOrCreate(1L, USD);
        t.deposit(first, 10);
        // new keys without a resize keep it valid
        for (long a = 2; a <= 10; a++) {
            t.getOrCreate(a, USD);
        }
        assertBalance(t, first, 10, 0);
        for (long a = 11; a <= 100; a++) {
            t.getOrCreate(a, USD);
        }
        assertThrows(IllegalArgumentException.class, () -> t.available(first));
        assertThrows(IllegalArgumentException.class, () -> t.deposit(first, 1));
        assertThrows(IllegalArgumentException.class, () -> t.reserve(first, 1));
        assertBalance(t, t.find(1L, USD), 10, 0);
    }

    @Test
    public void aReferenceIsStaleAfterARemoval() {
        final BalanceTable t = table(2);
        final long a = t.getOrCreate(1L, USD);
        final long b = t.getOrCreate(2L, USD);
        t.deposit(a, 10);
        assertStatus(OK, t.remove(b));
        assertThrows(IllegalArgumentException.class, () -> t.available(a));
        assertBalance(t, t.find(1L, USD), 10, 0);
        assertEquals(NO_REF, t.find(2L, USD));
    }

    @Test
    public void onlyAZeroBalanceIsRemoved() {
        final BalanceTable t = table(2);
        long ref = t.getOrCreate(1L, USD);
        t.deposit(ref, 10);
        t.reserve(ref, 4);
        assertStatus(NOT_ZERO, t.remove(ref));
        t.settle(ref, 4);
        assertStatus(NOT_ZERO, t.remove(ref));
        t.withdraw(ref, 6);
        assertStatus(OK, t.remove(ref));
        assertEquals(NO_REF, t.find(1L, USD));
        assertEquals(0, t.size());
    }

    @Test
    public void aBatchAppliesAllOrNothing() {
        final BalanceTable t = table(2);
        t.deposit(t.getOrCreate(1L, USD), 100);
        final BalanceTable.Batch batch = t.newBatch(8);
        batch.add(1L, USD, -50, 0)      // fine
                .add(2L, USD, 50, 0)    // creates the balance
                .add(3L, USD, -1, 0);   // a missing balance has nothing
        assertStatus(INSUFFICIENT_AVAILABLE, t.apply(batch));
        assertEquals(2, batch.failedPosting());
        assertBalance(t, t.find(1L, USD), 100, 0);
        assertEquals(NO_REF, t.find(2L, USD));
        assertEquals(1, t.size());

        batch.clear();
        batch.add(1L, USD, -50, 0).add(2L, USD, 50, 0);
        assertStatus(OK, t.apply(batch));
        assertEquals(-1, batch.failedPosting());
        assertBalance(t, t.find(1L, USD), 50, 0);
        assertBalance(t, t.find(2L, USD), 50, 0);
        t.integrityCheck();
    }

    @Test
    public void aBatchTakesTheSameKeyInOrder() {
        final BalanceTable t = table(2);
        t.deposit(t.getOrCreate(1L, USD), 100);
        final BalanceTable.Batch batch = t.newBatch(4);
        batch.add(1L, USD, -60, 60).add(1L, USD, -60, 60);
        assertStatus(INSUFFICIENT_AVAILABLE, t.apply(batch));
        assertEquals(1, batch.failedPosting());
        assertBalance(t, t.find(1L, USD), 100, 0);

        batch.clear();
        batch.add(1L, USD, -60, 60).add(1L, USD, 10, -10);
        assertStatus(OK, t.apply(batch));
        assertBalance(t, t.find(1L, USD), 50, 50);
    }

    @Test
    public void aBatchUndoesTheBalancesItCreated() {
        final BalanceTable t = table(2);
        final BalanceTable.Batch batch = t.newBatch(4);
        batch.add(2L, USD, 10, 0).add(2L, USD, -5, 5).add(3L, USD, -1, 0);
        assertStatus(INSUFFICIENT_AVAILABLE, t.apply(batch));
        assertEquals(NO_REF, t.find(2L, USD));
        assertEquals(0, t.size());

        batch.clear();
        batch.add(2L, USD, 10, 0).add(2L, USD, -5, 5);
        assertStatus(OK, t.apply(batch));
        assertBalance(t, t.find(2L, USD), 5, 5);
        // a posting of nothing to a missing key creates nothing
        batch.clear();
        batch.add(9L, USD, 0, 0);
        assertStatus(OK, t.apply(batch));
        assertEquals(NO_REF, t.find(9L, USD));
    }

    @Test
    public void aBatchUndoesThroughAResize() {
        final BalanceTable t = BalanceTable.builder().keyLongs(2).expectedSize(16).build();
        for (long a = 1; a <= 5; a++) {
            t.deposit(t.getOrCreate(a, USD), a * 100);
        }
        final BalanceTable.Batch batch = t.newBatch(200);
        for (long a = 1; a <= 5; a++) {
            batch.add(a, USD, -a * 10, a * 10);
        }
        for (long a = 100; a < 190; a++) {
            batch.add(a, BTC, 1, 0);           // 90 new balances: the table grows
        }
        batch.add(1L, USD, -1000, 0);          // refused
        assertStatus(INSUFFICIENT_AVAILABLE, t.apply(batch));
        assertEquals(95, batch.failedPosting());
        assertEquals(5, t.size());
        for (long a = 1; a <= 5; a++) {
            assertBalance(t, t.find(a, USD), a * 100, 0);
        }
        assertEquals(NO_REF, t.find(100L, BTC));
        t.integrityCheck();
    }

    /** 16 slots, maxLoad 0.95: 14 balances; the batch that brings the 15th undoes the 14th it brought */
    @Test
    public void aBatchThatDoesNotFitChangesNothing() {
        final BalanceTable t = BalanceTable.builder().keyLongs(1).expectedSize(9).fixedCapacity().build();
        for (long a = 1; a <= 13; a++) {
            t.deposit(t.getOrCreate(a), 1);
        }
        final BalanceTable.Batch batch = t.newBatch(4);
        batch.add(1L, -1, 0).add(100L, 1, 0).add(101L, 1, 0);
        assertThrows(IllegalStateException.class, () -> t.apply(batch));
        assertEquals(2, batch.failedPosting());
        assertEquals(13, t.size());
        assertBalance(t, t.find(1L), 1, 0);
        assertEquals(NO_REF, t.find(100L));
        t.integrityCheck();
    }

    @Test
    public void batchMisuse() {
        final BalanceTable t = table(2);
        final BalanceTable.Batch batch = t.newBatch(2);
        assertThrows(IllegalArgumentException.class, () -> batch.add(1L, 5, 0));
        assertThrows(IllegalArgumentException.class, () -> batch.add(1L, 2L, 3L, 5, 0));
        batch.add(1L, USD, 1, 0).add(2L, USD, 1, 0);
        assertThrows(IllegalStateException.class, () -> batch.add(3L, USD, 1, 0));
        assertEquals(2, batch.size());
        assertThrows(IllegalArgumentException.class, () -> table(3).apply(batch));
        assertThrows(IllegalArgumentException.class, () -> t.newBatch(0));
        batch.clear();
        assertEquals(0, batch.size());
    }

    /**
     * The flow of a risk engine: the buyer holds the quote amount of an order, the seller the base amount; a trade
     * settles both holds and credits the other side minus the fee; the rest of the buyer's order is cancelled.
     */
    @Test
    public void anOrderTradeAndCancel() {
        final long buyer = 1;
        final long seller = 2;
        final long fees = 0;
        final BalanceTable t = table(2);
        t.deposit(t.getOrCreate(buyer, USD), 1_000);
        t.deposit(t.getOrCreate(seller, BTC), 10);

        // place: the buyer bids for 2 BTC at 250, the seller asks 1 BTC
        assertStatus(OK, t.reserve(t.find(buyer, USD), 500));
        assertStatus(INSUFFICIENT_AVAILABLE, t.reserve(t.find(buyer, USD), 501));
        assertStatus(OK, t.reserve(t.find(seller, BTC), 1));

        // trade: 1 BTC at 240 (better than the bid), fee 4 USD taken from the seller
        final BalanceTable.Batch trade = t.newBatch(8);
        trade.add(buyer, USD, 0, -240)
                .add(buyer, BTC, 1, 0)
                .add(seller, BTC, 0, -1)
                .add(seller, USD, 236, 0)
                .add(fees, USD, 4, 0);
        assertStatus(OK, t.apply(trade));

        // cancel: the rest of the hold comes back
        assertStatus(OK, t.release(t.find(buyer, USD), 260));

        assertBalance(t, t.find(buyer, USD), 760, 0);
        assertBalance(t, t.find(buyer, BTC), 1, 0);
        assertBalance(t, t.find(seller, USD), 236, 0);
        assertBalance(t, t.find(seller, BTC), 9, 0);
        assertBalance(t, t.find(fees, USD), 4, 0);
        // nothing appeared or vanished
        assertEquals(1_000, total(t, USD));
        assertEquals(10, total(t, BTC));
    }

    private static long total(BalanceTable t, long currency) {
        final long[] sum = {0};
        t.forEach(ref -> {
            if (t.key(ref, 1) == currency) {
                sum[0] += t.available(ref) + t.reserved(ref);
            }
        });
        return sum[0];
    }

    @Test
    public void forEachMayPost() {
        final BalanceTable t = table(1);
        for (long a = 0; a < 100; a++) {
            t.deposit(t.getOrCreate(a), a);
        }
        t.forEach(ref -> t.reserve(ref, t.available(ref) / 2));
        final long[] sums = new long[2];
        t.forEach(ref -> {
            sums[0] += t.available(ref);
            sums[1] += t.reserved(ref);
        });
        assertEquals(99 * 100 / 2, sums[0] + sums[1]);
        assertTrue(sums[1] > 0);
        t.integrityCheck();
    }

    @Test
    public void clearAndEnsureCapacityMakeTheReferencesStale() {
        final BalanceTable t = BalanceTable.builder().keyLongs(1).expectedSize(16).build();
        final long a = t.getOrCreate(1L);
        t.ensureCapacity(10);
        assertBalance(t, a, 0, 0);
        t.ensureCapacity(10_000);
        assertThrows(IllegalArgumentException.class, () -> t.available(a));
        final long b = t.getOrCreate(1L);
        t.clear();
        assertEquals(0, t.size());
        assertThrows(IllegalArgumentException.class, () -> t.available(b));
        assertEquals(NO_REF, t.find(1L));
    }

    @Test
    public void statusNames() {
        assertEquals("OK", BalanceTable.statusName(OK));
        assertEquals("INSUFFICIENT_AVAILABLE", BalanceTable.statusName(INSUFFICIENT_AVAILABLE));
        assertEquals("NOT_ZERO", BalanceTable.statusName(NOT_ZERO));
        assertEquals("UNKNOWN(99)", BalanceTable.statusName(99));
    }

    @Test
    public void builderRejectsBadParameters() {
        assertThrows(IllegalArgumentException.class, () -> BalanceTable.builder().build());
        assertThrows(IllegalArgumentException.class, () -> BalanceTable.builder().keyLongs(1).expectedSize(-1).build());
        assertThrows(IllegalArgumentException.class, () -> BalanceTable.builder().keyLongs(1).maxLoad(1.5).build());
        assertThrows(IllegalArgumentException.class, () -> BalanceTable.builder().keyLongs(1).maxCapacity(3).build());
        assertEquals(3, BalanceTable.builder().keyLongs(3).build().keyLongs());
    }

    /**
     * Random single operations and batches against a model - keys to (available, reserved) - with the totals
     * checked: what was deposited and not withdrawn is all there, nothing below 0.
     */
    @Test
    public void randomOperationsMatchTheModel() {
        for (int seed = 0; seed < 30; seed++) {
            runAgainstModel(seed);
        }
    }

    private static void runAgainstModel(long seed) {
        final Random rand = new Random(seed);
        final int keyLongs = 1 + rand.nextInt(3);
        final BalanceTable t = BalanceTable.builder().keyLongs(keyLongs).expectedSize(rand.nextInt(40)).build();
        final Map<List<Long>, long[]> model = new HashMap<>();
        final BalanceTable.Batch batch = t.newBatch(6);
        final String context = "seed=" + seed + " keyLongs=" + keyLongs;
        long external = 0;

        for (int op = 0; op < 10_000; op++) {
            final long[] key = randomKey(rand, keyLongs);
            final int choice = rand.nextInt(100);
            if (choice < 15) {
                final long amount = rand.nextInt(1000);
                assertStatus(OK, t.deposit(t.getOrCreate(key, 0), amount));
                model.computeIfAbsent(boxed(key), k -> new long[2])[0] += amount;
                external += amount;
            } else if (choice < 70) {
                final long ref = t.find(key, 0);
                final long[] balance = model.get(boxed(key));
                final long amount = rand.nextInt(300);
                final int kind = rand.nextInt(5);
                final long dA = kind == 0 ? -amount : kind == 1 ? -amount : kind == 2 ? amount : 0;
                final long dR = kind == 0 ? 0 : kind == 1 ? amount : kind == 2 ? -amount : -amount;
                final int expected = expectedStatus(balance, dA, dR);
                final int actual = switch (kind) {
                    case 0 -> t.withdraw(ref, amount);
                    case 1 -> t.reserve(ref, amount);
                    case 2 -> t.release(ref, amount);
                    case 3 -> t.settle(ref, amount);
                    default -> t.post(ref, dA, dR);
                };
                assertEquals(context + " op " + op, BalanceTable.statusName(expected), BalanceTable.statusName(actual));
                if (expected == OK && balance != null) {
                    balance[0] += dA;
                    balance[1] += dR;
                    // what left the balances: a reservation or a release moves within one
                    external += dA + dR;
                }
            } else if (choice < 90) {
                batch.clear();
                final int n = 1 + rand.nextInt(5);
                final Map<List<Long>, long[]> after = copy(model);
                int expected = OK;
                long flow = 0;
                for (int i = 0; i < n; i++) {
                    final long[] k = randomKey(rand, keyLongs);
                    final long dA = rand.nextInt(400) - 200;
                    final long dR = rand.nextInt(400) - 200;
                    batch.add(k, 0, dA, dR);
                    if (expected == OK) {
                        final long[] balance = after.get(boxed(k));
                        expected = expectedStatus(balance, dA, dR);
                        if (expected == NO_BALANCE) {
                            expected = OK;  // a batch creates it
                            after.put(boxed(k), new long[]{dA, dR});
                            flow += dA + dR;
                        } else if (expected == OK && balance != null) {
                            balance[0] += dA;
                            balance[1] += dR;
                            flow += dA + dR;
                        }
                    }
                }
                assertEquals(context + " op " + op, BalanceTable.statusName(expected),
                        BalanceTable.statusName(t.apply(batch)));
                if (expected == OK) {
                    model.clear();
                    model.putAll(after);
                    external += flow;
                }
            } else {
                final long ref = t.find(key, 0);
                final long[] balance = model.get(boxed(key));
                final int expected = balance == null ? NO_BALANCE
                        : balance[0] == 0 && balance[1] == 0 ? OK : NOT_ZERO;
                assertStatus(expected, t.remove(ref));
                if (expected == OK) {
                    model.remove(boxed(key));
                }
            }
            assertEquals(context, model.size(), t.size());
        }
        t.integrityCheck();
        long total = 0;
        for (Map.Entry<List<Long>, long[]> e : model.entrySet()) {
            final long[] k = e.getKey().stream().mapToLong(Long::longValue).toArray();
            final long ref = t.find(k, 0);
            assertBalance(t, ref, e.getValue()[0], e.getValue()[1]);
            total += e.getValue()[0] + e.getValue()[1];
        }
        assertEquals(context, external, total);
    }

    /** a single posting to the model's balance (null: missing) */
    private static int expectedStatus(long[] balance, long dA, long dR) {
        if (balance == null) {
            if (dA < 0) {
                return INSUFFICIENT_AVAILABLE;
            }
            if (dR < 0) {
                return INSUFFICIENT_RESERVED;
            }
            return dA == 0 && dR == 0 ? OK : NO_BALANCE;
        }
        if (balance[0] + dA < 0) {
            return INSUFFICIENT_AVAILABLE;
        }
        if (balance[1] + dR < 0) {
            return INSUFFICIENT_RESERVED;
        }
        return OK;
    }

    private static long[] randomKey(Random rand, int keyLongs) {
        final long[] key = new long[keyLongs];
        for (int i = 0; i < keyLongs; i++) {
            key[i] = rand.nextInt(4);
        }
        return key;
    }

    private static List<Long> boxed(long[] key) {
        final List<Long> list = new ArrayList<>();
        for (long k : key) {
            list.add(k);
        }
        return list;
    }

    private static Map<List<Long>, long[]> copy(Map<List<Long>, long[]> model) {
        final Map<List<Long>, long[]> copy = new HashMap<>();
        model.forEach((k, v) -> copy.put(k, Arrays.copyOf(v, 2)));
        return copy;
    }
}
