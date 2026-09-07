package exchange.core2.collections.hashtable;

import org.junit.Before;
import org.junit.Test;

public class LongLongHashtableTest extends LongLongHashtableAbstractTest{

    @Before
    public void before() {
        hashtable = new LongLongHashtable();
    }


    @Test
    public void should_migrate_mas() {
        int initialCapacity = 16;

        hashtable = new LongLongHashtable();

        hashtable.put(15417, 1);
        hashtable.put(28723, 2);
        hashtable.put(43344, 3);
        hashtable.put(88455, 4);
        hashtable.put(23543, 5);
        hashtable.put(93234, 6);
        hashtable.put(79555, 7);
        hashtable.put(53733, 8);
        hashtable.put(27863, 9);

        LongLongHashtable dst = new LongLongHashtable();

        ((LongLongHashtable)hashtable).extractMatching(dst, 0x8000_0000, 0x8000_0000);


        // TODO finalize test
    }


}
