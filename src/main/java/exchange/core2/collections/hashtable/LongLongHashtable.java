package exchange.core2.collections.hashtable;

import exchange.core2.collections.orderbook.naive.OrderBookNaiveImpl;
import org.agrona.collections.Hashing;
import org.agrona.collections.LongLongConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.LongStream;

public class LongLongHashtable implements ILongLongHashtable {

    private static final Logger log = LoggerFactory.getLogger(OrderBookNaiveImpl.class);

    private static final long NOT_ALLOWED_KEY = 0L;

    long[] data = new long[32];
    long size = 0;
    int mask = 16 - 1;
    int upsizeThreshold = 10;


    @Override
    public long put(long key, long value) {
//        log.debug("PUT key:{} val:{}", key, value);
        final int pos = findFreePosition(key, data, mask);
        final long prevValue = data[pos + 1];
        if (data[pos] != key) {
            size++;
        }
        data[pos] = key;
        data[pos + 1] = value;

        if (size >= upsizeThreshold) {
            resize();
        }

        return prevValue;
    }


    @Override
    public long get(long key) {
//        log.debug("GET key:{}", key);
        final int pos = findFreePosition(key, data, mask);
        return data[pos + 1];
    }

    @Override
    public boolean containsKey(long key) {
        return get(key) != NOT_ALLOWED_KEY;
    }

    @Override
    public long remove(long key) {

        final int hash = Hashing.hash(key);
        int lastPos = (hash & mask);
//        log.debug("REMOVE key:{} hash:{}(0x{})->lastPos:{}(0x{})", key, hash, String.format("%x", hash), lastPos, String.format("%x", lastPos));


        // try all keys until either gap (NOT_ALLOWED_KEY)
        long existingKey = data[lastPos << 1];
        int gapPos = -1;
        long oldValue = 0L;
        while (true) {

            if (existingKey == key) {
                // desired key found
                gapPos = lastPos;
                oldValue = data[(lastPos << 1) + 1];
                size--;
//                log.debug("found prev value at gapPos={} oldValue={}", gapPos, oldValue);
            }

            // try next element
            final int posNext = (lastPos + 1) & mask;
            if (data[posNext << 1] == NOT_ALLOWED_KEY) {
//                log.debug("done probing, {} is empty", posNext);
                break;
            } else {
                existingKey = data[posNext << 1];
                lastPos = posNext;
//                log.debug("try next pos={} existingKey={}", lastPos, existingKey);
            }
        }

        if (gapPos == -1) {
//            log.debug("nothing to remove, returning 0");
            // nothing to remove - can just return
            return 0L;
        }

        // doing cleanup starting from last entry (pos)
//        log.debug("last pos={}", lastPos);

        // move gap to the right until it is at the last position
        while (gapPos != lastPos) {

            // find the greatest entry in a series that can fill the gap (hash < gap)
            int p = lastPos;
            while (true) {
                int h = Hashing.hash(data[p << 1]) & mask;
                boolean canFillGap = canFillGapAndFinish(p, h, gapPos, mask);
//                log.debug("try p={} h={} gapPos={}, canFillGapAndFinish={} ", p, h, gapPos, canFillGap);
                if (canFillGap) {
                    break;
                }

                p = (p - 1) & mask;

                if (p == gapPos) {
                    // reached beginning of series (all entries has desired position after the gap)
//                    log.debug("final x=lastPos={}", gapPos);
                    data[gapPos << 1] = NOT_ALLOWED_KEY;
                    data[(gapPos << 1) + 1] = 0;

                    return oldValue;
                }
            }

            // fill gap with movable entry
            data[gapPos << 1] = data[p << 1];
            data[(gapPos << 1) + 1] = data[(p << 1) + 1];

            gapPos = p;

//            log.debug("new gapPos={}", gapPos);
        }

//        log.debug("final gapPos=lastPos={}", gapPos);

        // safe to mark gap as empty and finish
        data[gapPos << 1] = NOT_ALLOWED_KEY;
        data[(gapPos << 1) + 1] = 0;

//        log.debug("return oldValue={}", oldValue);
        return oldValue;

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

    private void resize() {

        log.debug("RESIZE {}->{} elements={} ...", data.length, data.length * 2, size);

        final long[] data2 = new long[data.length * 2];
        final int newMask = data.length - 1;

        for (int i = 0; i < data.length; i += 2) {
            final long key = data[i];
            if (key == NOT_ALLOWED_KEY) {
                continue;
            }
            final int pos = findFreePosition(key, data2, newMask);
            data2[pos] = key;
            data2[pos + 1] = data[i + 1];
        }

        data = data2;
        mask = newMask;
        upsizeThreshold = (int) ((mask + 1) * 0.65);
        log.debug("RESIZE done, upsizeThreshold=" + upsizeThreshold);
    }

    private static int findFreePosition(long key, long[] data, int mask) {
        final int hash = Hashing.hash(key);
        int pos = (hash & mask) << 1;
//        log.debug("collision on key:{} hash:{}(0x{})->pos:{}(0x{})", key, hash, String.format("%x", hash), pos, String.format("%x", pos));
        long existingKey = data[pos];
        while (existingKey != NOT_ALLOWED_KEY && existingKey != key) {
            // try next element
            pos += 2;
            if (pos == data.length) {
                pos = 0;
            }
            existingKey = data[pos];

//            log.debug("try next pos={}", pos);
        }
        return pos;
    }

    void integrityCheck() {
        // TODO check all keys are reachable
        // TODO check size is correct
        // TODO check load factor is correct


    }

//    private int desiredPosition(long key) {
//        final int hash = Hashing.hash(key);
//        return (hash & mask) << 1;
//    }


    static boolean canFillGapAndFinish(int k, int h, int g, int mask) {
        return ((k - h) & mask) >= ((g - h) & mask);
    }
}
