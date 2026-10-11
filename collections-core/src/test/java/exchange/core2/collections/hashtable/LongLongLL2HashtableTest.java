package exchange.core2.collections.hashtable;

/**
 * The contract tests against LongLongLL2Hashtable. With the default executor and sync-resize bound, the tables
 * here resize synchronously up to 8200 entries and asynchronously past that (should_upsize goes to 50000).
 */
public class LongLongLL2HashtableTest extends LongLongHashtableAbstractTest {

    @Override
    protected ILongLongHashtable create(int expectedSize) {
        return new LongLongLL2Hashtable(expectedSize);
    }
}
