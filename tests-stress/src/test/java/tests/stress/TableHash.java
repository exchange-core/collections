package tests.stress;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

/**
 * The hash of the hashtables, package-private in the library: crafted keys must land exactly where the tables put
 * them, so the tests call the library's own function (and use its seed) instead of a copy.
 */
final class TableHash {

    private static final Class<?> UTILS = utils();
    private static final MethodHandle HASH = hashHandle();
    /** the arrays hold key ^ SEED (-Dexchange.hashtable.seed, default 0) */
    private static final long SEED = seed();

    private TableHash() {
    }

    /** the hash of the key as the tables compute it */
    static int hash(long key) {
        try {
            return (int) HASH.invokeExact(stored(key));
        } catch (Throwable e) {
            throw new IllegalStateException(e);
        }
    }

    /** the key as the arrays hold it */
    static long stored(long key) {
        return key ^ SEED;
    }

    private static Class<?> utils() {
        try {
            return Class.forName("exchange.core2.collections.hashtable.HashingUtils");
        } catch (ClassNotFoundException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static MethodHandle hashHandle() {
        try {
            return MethodHandles.privateLookupIn(UTILS, MethodHandles.lookup())
                    .findStatic(UTILS, "hash", MethodType.methodType(int.class, long.class));
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static long seed() {
        try {
            return (long) MethodHandles.privateLookupIn(UTILS, MethodHandles.lookup())
                    .findStaticVarHandle(UTILS, "SEED", long.class).get();
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
}
