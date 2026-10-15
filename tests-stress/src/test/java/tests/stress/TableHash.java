package tests.stress;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

/**
 * The hash of the hashtables, package-private in the library: crafted keys must land exactly where the tables put
 * them, so the tests call the library's own function instead of a copy.
 */
final class TableHash {

    private static final MethodHandle HASH = hashHandle();

    private TableHash() {
    }

    static int hash(long key) {
        try {
            return (int) HASH.invokeExact(key);
        } catch (Throwable e) {
            throw new IllegalStateException(e);
        }
    }

    private static MethodHandle hashHandle() {
        try {
            final Class<?> utils = Class.forName("exchange.core2.collections.hashtable.HashingUtils");
            return MethodHandles.privateLookupIn(utils, MethodHandles.lookup())
                    .findStatic(utils, "hash", MethodType.methodType(int.class, long.class));
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
}
