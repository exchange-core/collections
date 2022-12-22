package exchange.core2.collections.hashtable;

import org.agrona.collections.LongLongConsumer;

import java.util.stream.LongStream;

public interface ILongLongHashtable {

    long put(long key, long value);

    long get(long key);

    boolean containsKey(long key);

    long remove(long key);




    void clear();

    LongStream keysStream();

    LongStream valuesStream();

    void forEach(LongLongConsumer consumer);


}
