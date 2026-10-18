package exchange.core2.collections.records;

/**
 * The package-private integrity check of {@link RecordStore}, for the tests of other packages.
 */
public final class RecordStoreChecks {

    private RecordStoreChecks() {
    }

    public static void integrityCheck(RecordStore store) {
        store.integrityCheck();
    }
}
