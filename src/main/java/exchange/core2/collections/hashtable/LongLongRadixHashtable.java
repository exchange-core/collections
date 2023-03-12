package exchange.core2.collections.hashtable;

import org.agrona.BitUtil;
import org.agrona.collections.Hashing;
import org.agrona.collections.LongLongConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.LongStream;

public class LongLongRadixHashtable implements ILongLongHashtable {

    private static final Logger log = LoggerFactory.getLogger(LongLongRadixHashtable.class);

    private LongLongHashtable[] tables = new LongLongHashtable[4];
    private long size = 0;
    private int bitsShift = 30;

    private int upsizeCheckCounter = 1;
    private int upsizeCheckTable = 0;

    public LongLongRadixHashtable() {
        for (int i = 0; i < tables.length; i++) {
            tables[i] = new LongLongHashtable();
        }
    }

    @Override
    public long put(long key, long value) {

        if (upsizeCheckCounter++ == 1024) {
            upsizeCheckCounter = 0;

//            log.debug("checking subtable {} size...", upsizeCheckTable);

            final LongLongHashtable table = tables[upsizeCheckTable];
            //if (table.size() > table.upsizeThreshold()) {

            if (table.size() > 50_000_000) {
                upsize();
            }

            upsizeCheckTable = (upsizeCheckTable + 1) & (tables.length - 1);
        }

        final int hash = Hashing.hash(key);

        final int pos = (hash >>> bitsShift);

        // log.debug("hash={} pos={}", String.format("%08X", hash), pos);

        return tables[pos].put(key, value);
    }


    @Override
    public long get(long key) {
        return 0;
    }

    @Override
    public boolean containsKey(long key) {
        return false;
    }

    @Override
    public long remove(long key) {
        return 0;
    }

    @Override
    public void clear() {

    }

    @Override
    public LongStream keysStream() {
        return null;
    }

    @Override
    public LongStream valuesStream() {
        return null;
    }

    @Override
    public void forEach(LongLongConsumer consumer) {

    }

    @Override
    public long size() {
        return size;
    }

    private void upsize() {
        log.debug("Migrating sub-tables from {} to {}...", tables.length, tables.length * 2);

        final LongLongHashtable[] newTables = new LongLongHashtable[tables.length * 2];

        bitsShift--;

        final int destMask = -(1 << bitsShift);
        log.debug("destMask = {}", Integer.toBinaryString(destMask));

        for (int i = 0; i < tables.length; i++) {
            log.debug("split subtable {} ...", i);
            newTables[i * 2] = tables[i];
            final int expectedSize = (int) BitUtil.findNextPositivePowerOfTwo(tables[i].size() / 2);
            final LongLongHashtable newTable = new LongLongHashtable(expectedSize);
            newTables[i * 2 + 1] = newTable;
            final int destBits = (i * 2 + 1) << bitsShift;

            log.debug("  destBits = {}", Integer.toBinaryString(destBits));
            tables[i].extractMatching(newTable, destBits, destMask);

            log.debug("  source table size: {}, dst table size: {}", tables[i].size(), newTable.size());

        }
        tables = newTables;
        log.debug("Migrating completed");
    }
}
