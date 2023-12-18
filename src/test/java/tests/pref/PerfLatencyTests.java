package tests.pref;

import exchange.core2.collections.hashtable.ILongLongHashtable;
import exchange.core2.collections.hashtable.LongLongHashtable;
import exchange.core2.collections.hashtable.LongLongLLHashtable;
import org.HdrHistogram.Histogram;
import org.agrona.collections.Long2LongHashMap;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

public class PerfLatencyTests {
    private static final Logger log = LoggerFactory.getLogger(PerfLatencyTests.class);

    /*
    5000000: {50.0%=0.1us, 90.0%=0.2us, 95.0%=0.4us, 99.0%=0.7us, 99.9%=1.5us, 99.99%=13.2us, W=37us}
    6000000: {50.0%=0.1us, 90.0%=0.2us, 95.0%=0.4us, 99.0%=0.7us, 99.9%=1.5us, 99.99%=4.5us, W=177ms}
    7000000: {50.0%=0.1us, 90.0%=0.2us, 95.0%=0.4us, 99.0%=0.7us, 99.9%=1.7us, 99.99%=4.7us, W=36us}
    8000000: {50.0%=0.1us, 90.0%=0.2us, 95.0%=0.4us, 99.0%=0.7us, 99.9%=1.7us, 99.99%=4.7us, W=89us}
    9000000: {50.0%=0.1us, 90.0%=0.2us, 95.0%=0.4us, 99.0%=0.7us, 99.9%=1.8us, 99.99%=4.9us, W=109us}
    10000000: {50.0%=0.1us, 90.0%=0.2us, 95.0%=0.4us, 99.0%=0.7us, 99.9%=1.9us, 99.99%=4.7us, W=37us}
    11000000: {50.0%=0.1us, 90.0%=0.2us, 95.0%=0.4us, 99.0%=0.8us, 99.9%=2.0us, 99.99%=8.2us, W=323ms}
    12000000: {50.0%=0.1us, 90.0%=0.2us, 95.0%=0.4us, 99.0%=0.7us, 99.9%=2.0us, 99.99%=5.6us, W=89us}
    13000000: {50.0%=0.1us, 90.0%=0.2us, 95.0%=0.4us, 99.0%=0.7us, 99.9%=1.9us, 99.99%=5.2us, W=68us}
    14000000: {50.0%=0.1us, 90.0%=0.2us, 95.0%=0.4us, 99.0%=0.7us, 99.9%=1.8us, 99.99%=5.2us, W=75us}
    15000000: {50.0%=0.1us, 90.0%=0.2us, 95.0%=0.4us, 99.0%=0.7us, 99.9%=1.9us, 99.99%=5.0us, W=46us}
    16000000: {50.0%=0.1us, 90.0%=0.2us, 95.0%=0.4us, 99.0%=0.8us, 99.9%=2.0us, 99.99%=4.6us, W=82us}
    17000000: {50.0%=0.1us, 90.0%=0.2us, 95.0%=0.4us, 99.0%=0.8us, 99.9%=2.1us, 99.99%=4.9us, W=43us}
    18000000: {50.0%=0.1us, 90.0%=0.2us, 95.0%=0.5us, 99.0%=0.8us, 99.9%=2.2us, 99.99%=5.3us, W=39us}
    19000000: {50.0%=0.1us, 90.0%=0.2us, 95.0%=0.5us, 99.0%=1.0us, 99.9%=2.9us, 99.99%=20.7us, W=464us}
    20000000: {50.0%=0.1us, 90.0%=0.2us, 95.0%=0.5us, 99.0%=0.8us, 99.9%=2.1us, 99.99%=4.7us, W=36us}
    21000000: {50.0%=0.1us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=0.8us, 99.9%=2.1us, 99.99%=5.0us, W=30us}
    22000000: {50.0%=0.1us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=0.9us, 99.9%=2.2us, 99.99%=4.9us, W=661ms}
    23000000: {50.0%=0.1us, 90.0%=0.2us, 95.0%=0.4us, 99.0%=0.8us, 99.9%=2.0us, 99.99%=4.7us, W=43us}
    24000000: {50.0%=0.1us, 90.0%=0.2us, 95.0%=0.5us, 99.0%=0.9us, 99.9%=2.1us, 99.99%=5.1us, W=146us}
    25000000: {50.0%=0.1us, 90.0%=0.2us, 95.0%=0.5us, 99.0%=0.9us, 99.9%=2.1us, 99.99%=5.3us, W=47us}
    26000000: {50.0%=0.1us, 90.0%=0.2us, 95.0%=0.4us, 99.0%=0.8us, 99.9%=2.1us, 99.99%=5.3us, W=50us}
    27000000: {50.0%=0.1us, 90.0%=0.2us, 95.0%=0.5us, 99.0%=0.9us, 99.9%=2.0us, 99.99%=5.0us, W=59us}
    28000000: {50.0%=0.1us, 90.0%=0.2us, 95.0%=0.5us, 99.0%=0.9us, 99.9%=2.0us, 99.99%=4.9us, W=40us}
    29000000: {50.0%=0.1us, 90.0%=0.2us, 95.0%=0.5us, 99.0%=0.9us, 99.9%=2.1us, 99.99%=4.9us, W=37us}
    30000000: {50.0%=0.1us, 90.0%=0.2us, 95.0%=0.5us, 99.0%=0.9us, 99.9%=2.1us, 99.99%=4.7us, W=37us}
    31000000: {50.0%=0.1us, 90.0%=0.2us, 95.0%=0.5us, 99.0%=0.9us, 99.9%=2.1us, 99.99%=5.5us, W=473us}
    32000000: {50.0%=0.1us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=0.9us, 99.9%=2.1us, 99.99%=5.0us, W=39us}
    33000000: {50.0%=0.1us, 90.0%=0.2us, 95.0%=0.5us, 99.0%=0.8us, 99.9%=2.1us, 99.99%=5.1us, W=44us}
    34000000: {50.0%=0.1us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.0us, 99.9%=2.1us, 99.99%=5.4us, W=36us}
    35000000: {50.0%=0.1us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=0.9us, 99.9%=2.1us, 99.99%=5.2us, W=58us}
    36000000: {50.0%=0.1us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.0us, 99.9%=2.1us, 99.99%=4.9us, W=57us}
    37000000: {50.0%=0.1us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.0us, 99.9%=2.2us, 99.99%=4.9us, W=53us}
    38000000: {50.0%=0.1us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.2us, 99.9%=3.0us, 99.99%=19.7us, W=57us}
    39000000: {50.0%=0.1us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=0.9us, 99.9%=2.1us, 99.99%=5.1us, W=35us}
    40000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.2us, 99.9%=2.4us, 99.99%=6.2us, W=53us}
    41000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.2us, 99.9%=2.3us, 99.99%=5.4us, W=46us}
    42000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.0us, 99.9%=2.2us, 99.99%=5.4us, W=36us}
    43000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.0us, 99.9%=2.2us, 99.99%=5.0us, W=59us}
    44000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.2us, 99.9%=2.3us, 99.99%=6.2us, W=1.31s}
    45000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.2us, 99.9%=2.1us, 99.99%=5.1us, W=38us}
    46000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.1us, 99.9%=2.1us, 99.99%=5.1us, W=54us}
    47000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.2us, 99.9%=2.2us, 99.99%=5.2us, W=40us}
    48000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.2us, 99.9%=2.2us, 99.99%=5.1us, W=38us}
    49000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.2us, 99.9%=2.2us, 99.99%=5.1us, W=61us}
    50000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.2us, 99.9%=2.2us, 99.99%=5.2us, W=30us}
    51000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.2us, 99.9%=2.2us, 99.99%=5.2us, W=39us}
    52000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.2us, 99.9%=2.3us, 99.99%=5.5us, W=181us}
    53000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.2us, 99.9%=2.2us, 99.99%=5.6us, W=47us}
    54000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.1us, 99.9%=2.2us, 99.99%=5.0us, W=95us}
    55000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.2us, 99.9%=2.2us, 99.99%=5.5us, W=37us}
    56000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.2us, 99.9%=2.2us, 99.99%=5.5us, W=44us}
    57000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.2us, 99.9%=2.2us, 99.99%=5.8us, W=49us}
    58000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.2us, 99.9%=2.2us, 99.99%=5.4us, W=100us}
    59000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.2us, 99.9%=2.2us, 99.99%=5.1us, W=38us}
    60000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.2us, 99.9%=2.2us, 99.99%=5.2us, W=47us}
    61000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.2us, 99.9%=2.2us, 99.99%=5.4us, W=52us}
    62000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.2us, 99.9%=2.2us, 99.99%=5.6us, W=46us}
    63000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.2us, 99.9%=2.2us, 99.99%=5.2us, W=38us}
    64000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.3us, 99.99%=5.3us, W=90us}
    65000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.2us, 99.9%=2.2us, 99.99%=5.3us, W=80us}
    66000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.2us, 99.99%=5.3us, W=54us}
    67000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.3us, 99.99%=5.5us, W=198us}
    68000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.2us, 99.99%=5.1us, W=57us}
    69000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.2us, 99.99%=5.2us, W=62us}
    70000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.3us, 99.99%=5.7us, W=452us}
    71000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.2us, 99.99%=5.5us, W=50us}
    72000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.3us, 99.99%=5.6us, W=39us}
    73000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.2us, 99.99%=5.3us, W=39us}
    74000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.3us, 99.99%=5.3us, W=37us}
    75000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.5us, 99.99%=5.6us, W=119us}
    76000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.3us, 99.99%=5.5us, W=38us}
    77000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.2us, 99.99%=5.2us, W=39us}
    78000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.3us, 99.99%=5.3us, W=44us}
    79000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.3us, 99.99%=5.6us, W=39us}
    80000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.3us, 99.99%=5.4us, W=45us}
    81000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.4us, 99.99%=5.5us, W=162us}
    82000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.3us, 99.99%=5.4us, W=41us}
    83000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.4us, 99.99%=5.4us, W=66us}
    84000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.3us, 99.99%=5.2us, W=47us}
    85000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.7us, 99.99%=5.9us, W=87us}
    86000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.4us, 99.99%=5.5us, W=108us}
    87000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.3us, 99.99%=5.3us, W=66us}
    88000000: {50.0%=0.2us, 90.0%=0.2us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.1us, 99.99%=5.3us, W=2.62s}
    89000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.7us, 99.99%=5.5us, W=71us}
    90000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.2us, 99.99%=5.4us, W=50us}
    91000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.6us, 99.99%=6.1us, W=74us}
    92000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.2us, 99.99%=5.3us, W=32us}
    93000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.2us, 99.99%=5.1us, W=37us}
    94000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.3us, 99.99%=5.8us, W=54us}
    95000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.5us, 99.99%=5.4us, W=79us}
    96000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.5us, 99.99%=5.5us, W=40us}
    97000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.3us, 99.99%=5.5us, W=37us}
    98000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.6us, 99.99%=5.8us, W=37us}
    99000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.2us, 99.99%=5.3us, W=37us}
    100000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.2us, 99.99%=5.4us, W=35us}
    101000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.3us, 99.99%=5.4us, W=161us}
    102000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.4us, 99.99%=5.5us, W=52us}
    103000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.3us, 99.99%=5.6us, W=59us}
    104000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.2us, 99.99%=5.5us, W=40us}

    Process finished with exit code 0

     */
    @Test
    public void benchmarkBasic() {
        benchmarkAbstract(
            (long[] kv) -> {
                final ILongLongHashtable hashtable = new LongLongHashtable();
                for (long l : kv) hashtable.put(l, l);
                return hashtable;
            },
            this::benchmark,
            (ILongLongHashtable hashtable, long[] kv) -> {
                for (long l : kv) hashtable.put(l, l);
            }
        );
    }

    @Test
    public void benchmarkLL() {
        benchmarkAbstract(
            (long[] kv) -> {
                final ILongLongHashtable hashtable = new LongLongLLHashtable(5000000);
                for (long l : kv) hashtable.put(l, l);
                return hashtable;
            },
            this::benchmark,
            (ILongLongHashtable hashtable, long[] kv) -> {
                for (long l : kv) hashtable.put(l, l);
            }
        );
    }


    /*
5000000: {50.0%=0.1us, 90.0%=0.2us, 95.0%=0.4us, 99.0%=0.7us, 99.9%=1.6us, 99.99%=4.7us, W=43us}
6000000: {50.0%=0.1us, 90.0%=0.2us, 95.0%=0.4us, 99.0%=0.7us, 99.9%=1.5us, 99.99%=4.4us, W=211ms}
7000000: {50.0%=0.1us, 90.0%=0.2us, 95.0%=0.4us, 99.0%=0.7us, 99.9%=1.8us, 99.99%=4.3us, W=32us}
8000000: {50.0%=0.1us, 90.0%=0.2us, 95.0%=0.4us, 99.0%=0.7us, 99.9%=1.8us, 99.99%=4.6us, W=30us}
9000000: {50.0%=0.1us, 90.0%=0.2us, 95.0%=0.4us, 99.0%=0.8us, 99.9%=2.0us, 99.99%=4.8us, W=114us}
10000000: {50.0%=0.1us, 90.0%=0.2us, 95.0%=0.5us, 99.0%=0.8us, 99.9%=2.0us, 99.99%=4.8us, W=48us}
11000000: {50.0%=0.1us, 90.0%=0.2us, 95.0%=0.4us, 99.0%=0.8us, 99.9%=2.0us, 99.99%=4.8us, W=415ms}
12000000: {50.0%=0.1us, 90.0%=0.2us, 95.0%=0.4us, 99.0%=0.7us, 99.9%=1.9us, 99.99%=4.2us, W=35us}
13000000: {50.0%=0.1us, 90.0%=0.2us, 95.0%=0.4us, 99.0%=0.7us, 99.9%=2.0us, 99.99%=4.6us, W=35us}
14000000: {50.0%=0.1us, 90.0%=0.2us, 95.0%=0.4us, 99.0%=0.8us, 99.9%=1.9us, 99.99%=4.7us, W=30us}
15000000: {50.0%=0.1us, 90.0%=0.2us, 95.0%=0.4us, 99.0%=0.8us, 99.9%=2.0us, 99.99%=4.9us, W=121us}
16000000: {50.0%=0.1us, 90.0%=0.2us, 95.0%=0.4us, 99.0%=0.8us, 99.9%=2.0us, 99.99%=4.6us, W=44us}
17000000: {50.0%=0.1us, 90.0%=0.2us, 95.0%=0.4us, 99.0%=0.8us, 99.9%=2.2us, 99.99%=4.7us, W=37us}
18000000: {50.0%=0.1us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=0.8us, 99.9%=2.1us, 99.99%=5.0us, W=26.5us}
19000000: {50.0%=0.1us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=0.8us, 99.9%=2.0us, 99.99%=4.7us, W=43us}
20000000: {50.0%=0.1us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=0.9us, 99.9%=2.1us, 99.99%=4.8us, W=25.9us}
21000000: {50.0%=0.1us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=0.9us, 99.9%=2.1us, 99.99%=4.7us, W=63us}
22000000: {50.0%=0.1us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=0.9us, 99.9%=2.1us, 99.99%=5.1us, W=799ms}
23000000: {50.0%=0.1us, 90.0%=0.2us, 95.0%=0.4us, 99.0%=0.8us, 99.9%=2.1us, 99.99%=4.6us, W=39us}
24000000: {50.0%=0.1us, 90.0%=0.2us, 95.0%=0.4us, 99.0%=0.9us, 99.9%=2.1us, 99.99%=4.7us, W=52us}
25000000: {50.0%=0.1us, 90.0%=0.2us, 95.0%=0.4us, 99.0%=0.8us, 99.9%=2.1us, 99.99%=4.8us, W=33us}
26000000: {50.0%=0.1us, 90.0%=0.2us, 95.0%=0.4us, 99.0%=0.8us, 99.9%=2.1us, 99.99%=5.2us, W=36us}
27000000: {50.0%=0.1us, 90.0%=0.2us, 95.0%=0.4us, 99.0%=0.8us, 99.9%=2.1us, 99.99%=5.1us, W=62us}
28000000: {50.0%=0.1us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=0.9us, 99.9%=2.2us, 99.99%=4.8us, W=38us}
29000000: {50.0%=0.1us, 90.0%=0.2us, 95.0%=0.5us, 99.0%=0.9us, 99.9%=2.2us, 99.99%=5.0us, W=27.1us}
30000000: {50.0%=0.1us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.0us, 99.9%=2.2us, 99.99%=5.2us, W=67us}
31000000: {50.0%=0.1us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.0us, 99.9%=2.2us, 99.99%=4.9us, W=36us}
32000000: {50.0%=0.1us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=0.9us, 99.9%=2.2us, 99.99%=5.2us, W=37us}
33000000: {50.0%=0.1us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.1us, 99.9%=2.2us, 99.99%=4.8us, W=67us}
34000000: {50.0%=0.1us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.0us, 99.9%=2.1us, 99.99%=5.0us, W=37us}
35000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.1us, 99.9%=2.2us, 99.99%=5.0us, W=35us}
36000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.1us, 99.9%=2.2us, 99.99%=5.0us, W=45us}
37000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.1us, 99.9%=2.2us, 99.99%=5.1us, W=48us}
38000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.1us, 99.9%=2.3us, 99.99%=5.1us, W=67us}
39000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.1us, 99.9%=2.2us, 99.99%=5.1us, W=52us}
40000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.2us, 99.9%=2.3us, 99.99%=5.3us, W=203us}
41000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.2us, 99.9%=2.2us, 99.99%=5.1us, W=43us}
42000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.2us, 99.9%=2.3us, 99.99%=5.1us, W=46us}
43000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.2us, 99.9%=2.3us, 99.99%=5.0us, W=44us}
44000000: {50.0%=0.1us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.1us, 99.9%=2.2us, 99.99%=5.3us, W=1.59s}
45000000: {50.0%=0.1us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.2us, 99.9%=2.2us, 99.99%=5.0us, W=49us}
46000000: {50.0%=0.1us, 90.0%=0.2us, 95.0%=0.5us, 99.0%=1.1us, 99.9%=2.2us, 99.99%=5.2us, W=38us}
47000000: {50.0%=0.1us, 90.0%=0.2us, 95.0%=0.5us, 99.0%=1.1us, 99.9%=2.2us, 99.99%=5.0us, W=51us}
48000000: {50.0%=0.1us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.2us, 99.9%=2.3us, 99.99%=5.8us, W=157us}
49000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.1us, 99.9%=2.2us, 99.99%=4.9us, W=33us}
50000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.2us, 99.9%=2.2us, 99.99%=5.0us, W=44us}
51000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.2us, 99.9%=2.2us, 99.99%=5.2us, W=149us}
52000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.2us, 99.99%=5.1us, W=71us}
53000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.2us, 99.9%=2.2us, 99.99%=4.9us, W=68us}
54000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.2us, 99.9%=2.2us, 99.99%=5.0us, W=66us}
55000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.2us, 99.9%=2.2us, 99.99%=5.2us, W=52us}
56000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.2us, 99.99%=5.1us, W=48us}
57000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.2us, 99.9%=2.2us, 99.99%=5.1us, W=35us}
58000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.3us, 99.99%=4.9us, W=44us}
59000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.3us, 99.99%=4.9us, W=47us}
60000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.2us, 99.9%=2.2us, 99.99%=5.0us, W=38us}
61000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.3us, 99.99%=5.1us, W=36us}
62000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.3us, 99.99%=5.2us, W=45us}
63000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.3us, 99.99%=4.9us, W=28.3us}
64000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.4us, 99.99%=5.3us, W=37us}
65000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.4us, 99.99%=5.4us, W=90us}
66000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.3us, 99.99%=6.0us, W=446us}
67000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.4us, 99.99%=5.2us, W=112us}
68000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.3us, 99.99%=5.0us, W=50us}
69000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.3us, 99.99%=5.1us, W=50us}
70000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.4us, 99.99%=5.2us, W=38us}
71000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.3us, 99.99%=5.0us, W=26.1us}
72000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.4us, 99.99%=5.3us, W=45us}
73000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.4us, 99.99%=5.5us, W=86us}
74000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.4us, 99.99%=5.3us, W=39us}
75000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.6us, 99.99%=5.8us, W=37us}
76000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.4us, 99.99%=5.3us, W=47us}
77000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.4us, 99.99%=5.4us, W=47us}
78000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.5us, 99.99%=5.8us, W=48us}
79000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.4us, 99.9%=2.5us, 99.99%=5.3us, W=118us}
80000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.6us, 99.99%=5.6us, W=40us}
81000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.7us, 99.99%=5.3us, W=46us}
82000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.4us, 99.9%=2.6us, 99.99%=5.8us, W=49us}
83000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.5us, 99.99%=5.5us, W=114us}
84000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.4us, 99.99%=5.2us, W=32us}
85000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.4us, 99.9%=2.5us, 99.99%=5.5us, W=121us}
86000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.4us, 99.9%=2.5us, 99.99%=5.3us, W=27.1us}
87000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.4us, 99.9%=2.5us, 99.99%=5.6us, W=46us}
88000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.2us, 99.99%=5.0us, W=3.2s}
89000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.3us, 99.99%=5.3us, W=72us}
90000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.3us, 99.99%=5.5us, W=37us}
91000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.4us, 99.99%=5.2us, W=52us}
92000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.3us, 99.99%=5.2us, W=75us}
93000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=3.3us, 99.99%=5.1us, W=40us}
94000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.3us, 99.99%=5.2us, W=62us}
95000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.2us, 99.99%=5.1us, W=26.5us}
96000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.4us, 99.99%=5.1us, W=44us}
97000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.5us, 99.99%=5.5us, W=190us}
98000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.3us, 99.99%=5.4us, W=55us}
99000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.3us, 99.99%=5.0us, W=139us}
100000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.3us, 99.99%=5.1us, W=43us}
101000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.3us, 99.99%=5.1us, W=35us}
102000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.2us, 99.99%=5.0us, W=37us}
103000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.3us, 99.99%=5.1us, W=39us}
104000000: {50.0%=0.2us, 90.0%=0.3us, 95.0%=0.5us, 99.0%=1.3us, 99.9%=2.8us, 99.99%=14.1us, W=488us}
     */
    @Test
    public void benchmarkAgrona() {
        benchmarkAbstract(
            (long[] kv) -> {
                final Long2LongHashMap hashtable = new Long2LongHashMap(0L);
                for (long l : kv) hashtable.put(l, l);
                return hashtable;
            },
            this::benchmark,
            (Long2LongHashMap hashtable, long[] kv) -> {
                for (long l : kv) hashtable.put(l, l);
            }
        );
    }

    /*
25200000: {50.0%=0.6us, 90.0%=12.2ms, 95.0%=13.6ms, 99.0%=14.8ms, 99.9%=15.1ms, 99.99%=15.1ms, W=1.46s}
27900000: {50.0%=6.5ms, 90.0%=19.3ms, 95.0%=603ms, 99.0%=605ms, 99.9%=605ms, 99.99%=605ms, W=605ms}
50400000: {50.0%=6.6ms, 90.0%=19.9ms, 95.0%=21.7ms, 99.0%=22.9ms, 99.9%=23.2ms, 99.99%=23.2ms, W=3.5s}
64300000: {50.0%=10.1ms, 90.0%=922ms, 95.0%=924ms, 99.0%=925ms, 99.9%=926ms, 99.99%=926ms, W=926ms}
100700000: {50.0%=1.0us, 90.0%=9.2ms, 95.0%=10.9ms, 99.0%=12.2ms, 99.9%=12.5ms, 99.99%=12.5ms, W=8.1s}
168900000: {50.0%=18.2ms, 90.0%=39ms, 95.0%=41ms, 99.0%=1.35s, 99.9%=1.35s, 99.99%=1.35s, W=1.35s}
     */
    @Test
    public void benchmarkStdHashMap() {
        benchmarkAbstract(
            (long[] kv) -> {
                final Map<Long, Long> hashtable = new HashMap<>(5000000);
                for (long l : kv) hashtable.put(l, l);
                return hashtable;
            },
            this::benchmark,
            (Map<Long, Long> hashtable, long[] kv) -> {
                for (long l : kv) hashtable.put(l, l);
            }
        );
    }

    /*
12600000: {50.0%=0.4us, 90.0%=5.8ms, 95.0%=9.3ms, 99.0%=11.9ms, 99.9%=12.5ms, 99.99%=12.6ms, W=1.29s}
25200000: {50.0%=0.6us, 90.0%=10.9ms, 95.0%=14.1ms, 99.0%=16.6ms, 99.9%=17.0ms, 99.99%=17.1ms, W=3.6s}
50400000: {50.0%=8.4ms, 90.0%=23.9ms, 95.0%=25.7ms, 99.0%=27.1ms, 99.9%=27.5ms, 99.99%=27.5ms, W=5.1s}
100700000: {50.0%=0.7us, 90.0%=11.9ms, 95.0%=14.0ms, 99.0%=15.5ms, 99.9%=15.8ms, 99.99%=15.8ms, W=12.9s}
     */
    @Test
    public void benchmarkStdCHM() {
        benchmarkAbstract(
            (long[] kv) -> {
                final Map<Long, Long> hashtable = new ConcurrentHashMap<>(5000000);
                for (long l : kv) hashtable.put(l, l);
                return hashtable;
            },
            this::benchmark,
            (Map<Long, Long> hashtable, long[] kv) -> {
                for (long l : kv) hashtable.put(l, l);
            }
        );
    }


    private <T> void benchmarkAbstract(Function<long[], T> factory,
                                       BiFunction<T, long[], SingleResult> singleTest,
                                       BiConsumer<T, long[]> extraLoader) {
        int n = 4_000_000;
        long seed = 2918723469278364978L;


        log.debug("Pre-filling {} random k/v pairs...", n);
        Random rand = new Random(seed);
        final long[] prefillKeys = new long[n];
        for (int i = 0; i < n; i++) prefillKeys[i] = rand.nextLong();
//
//
//        for (int i = 0; i < n; i++) {
//            final long key = rand.nextLong();
//            final long value = rand.nextLong();
//            hashtable.put(key, value);
//        }

        int n2 = 100_000;

        final T hashtable = factory.apply(prefillKeys);
        log.debug("Benchmarking...");

        final long[] keys = new long[n2];

        // TODO make continuous test (non-stop)

        for (int j = 0; j < 1780; j++) {
            for (int i = 0; i < n2; i++) keys[i] = rand.nextLong();
            final SingleResult benchmark = singleTest.apply(hashtable, keys);
            log.info("{}: {}", benchmark.size, LatencyTools.createLatencyReportFast(benchmark.avgGet));
            extraLoader.accept(hashtable, keys);
        }


//
//        log.info("done");
    }

    /*

21:30:50.390 87100000: {50.0%=0.2us, 90.0%=0.7us, 95.0%=1.2us, 99.0%=2.1us, 99.9%=7.1us, 99.99%=16.3us, W=32us}
21:30:50.509 87200000: {50.0%=0.2us, 90.0%=0.7us, 95.0%=1.1us, 99.0%=1.8us, 99.9%=5.0us, 99.99%=14.3us, W=33us}
21:30:50.567 - (A) ----------- starting async migration capacity: 134217728->268435456 -----------------
21:30:50.567 table - Resize preparation: 134us
21:30:50.596 r - (A) Allocating array: long[536870912] ...
21:30:50.677 87300000: {50.0%=6.0ms, 90.0%=34ms, 95.0%=37ms, 99.0%=41ms, 99.9%=42ms, 99.99%=42ms, W=42ms}
21:30:50.956 87400000: {50.0%=7.7ms, 90.0%=24.0ms, 95.0%=28.3ms, 99.0%=34ms, 99.9%=35ms, 99.99%=35ms, W=35ms}
21:30:51.235 87500000: {50.0%=7.6ms, 90.0%=29.2ms, 95.0%=36ms, 99.0%=41ms, 99.9%=43ms, 99.99%=43ms, W=43ms}
21:30:51.544 87600000: {50.0%=8.6ms, 90.0%=29.6ms, 95.0%=34ms, 99.0%=39ms, 99.9%=41ms, 99.99%=41ms, W=41ms}
21:30:51.818 87700000: {50.0%=7.1ms, 90.0%=22.1ms, 95.0%=27.1ms, 99.0%=33ms, 99.9%=34ms, 99.99%=34ms, W=34ms}
21:30:52.107 87800000: {50.0%=8.0ms, 90.0%=29.5ms, 95.0%=33ms, 99.0%=37ms, 99.9%=39ms, 99.99%=39ms, W=39ms}
21:30:52.344 r - (A) Allocated new array, first gap g0=0, copying...
21:30:52.344 r - (A) Next segment after 0...
21:30:52.412 87900000: {50.0%=8.9ms, 90.0%=44ms, 95.0%=56ms, 99.0%=67ms, 99.9%=69ms, 99.99%=69ms, W=69ms}
21:30:52.855 88000000: {50.0%=24.5ms, 90.0%=60ms, 95.0%=70ms, 99.0%=78ms, 99.9%=81ms, 99.99%=81ms, W=81ms}
21:30:53.206 88100000: {50.0%=19.0ms, 90.0%=43ms, 95.0%=46ms, 99.0%=49ms, 99.9%=50ms, 99.99%=50ms, W=50ms}
21:30:53.457 88200000: {50.0%=2.37ms, 90.0%=8.0ms, 95.0%=9.7ms, 99.0%=11.3ms, 99.9%=11.8ms, 99.99%=11.9ms, W=11.9ms}
21:30:53.668 88300000: {50.0%=2.3us, 90.0%=547us, 95.0%=1.1ms, 99.0%=2.05ms, 99.9%=2.51ms, 99.99%=2.59ms, W=2.61ms}
21:30:53.841 88400000: {50.0%=0.6us, 90.0%=20.4us, 95.0%=46us, 99.0%=106us, 99.9%=148us, 99.99%=176us, W=184us}
21:30:53.986 88500000: {50.0%=0.2us, 90.0%=2.1us, 95.0%=4.2us, 99.0%=198us, 99.9%=548us, 99.99%=603us, W=610us}
21:30:54.075 r - (A) Copying completed gp=0 pauseResponse=3563620 ----------------------
21:30:54.110 88600000: {50.0%=0.2us, 90.0%=0.9us, 95.0%=1.3us, 99.0%=3.4us, 99.9%=13.8us, 99.99%=29.3us, W=34us}
21:30:54.220 88700000: {50.0%=0.2us, 90.0%=0.8us, 95.0%=1.2us, 99.0%=2.4us, 99.9%=7.6us, 99.99%=24.9us, W=34us}

     */

    private SingleResult benchmark(ILongLongHashtable hashtable, long[] keys) {

        int tps = 1_000_000;


        final Histogram histogramPut = new Histogram(60_000_000_000L, 3);

        final long picosPerCmd = (1024L * 1_000_000_000L) / tps;
        final long startTimeNs = System.nanoTime();

        long planneTimeOffsetPs = 0L;
        long lastKnownTimeOffsetPs = 0L;

        //int nanoTimeRequestsCounter = 0;

        for (int i = 0; i < keys.length; i++) {

            final long key = keys[i];

            planneTimeOffsetPs += picosPerCmd;

            while (planneTimeOffsetPs > lastKnownTimeOffsetPs) {

                lastKnownTimeOffsetPs = (System.nanoTime() - startTimeNs) << 10;

                // nanoTimeRequestsCounter++;

                // spin until its time to send next command
                Thread.onSpinWait(); // 1us-26  max34
                // LockSupport.parkNanos(1L); // 1us-25 max29
                // Thread.yield();   // 1us-28  max32
            }

            hashtable.put(key, key);
            final long putNs = System.nanoTime() - startTimeNs - (lastKnownTimeOffsetPs >> 10);

            histogramPut.recordValue(putNs);
        }


//        for (long key : keys) {
//            long t = System.nanoTime();
//            hashtable.put(key, key);
//            long putNs = (System.nanoTime() - t);
//            histogramPut.recordValue(putNs);
//
//            if (putNs > 1_000_000) {
//                log.debug("{}: took too long {}ms, key={} ", hashtable.size(), putNs / 1000000, key);
//            }
//        }


//        t = System.nanoTime();
//        long acc = 0;
//        for (long key : keys) acc += hashtable.get(key);
//        long getNs = (System.nanoTime() - t) / keys.length;
//
//        t = System.nanoTime();
//        for (long key : keys) hashtable.remove(key);
//        long removNs = (System.nanoTime() - t) / keys.length;

//        log.info("validating remove...");
//        Random rand = new Random(keys[0]);
//        for (int i = 0; i < keys.length; i++) {
//            final long key = rand.nextLong();
//            final long value = rand.nextLong();
//            assertThat(hashtable.remove(key), Is.is(value));
//        }
//
//        log.info("confirm empty...");
//        rand = new Random(keys[0]);
//        for (int i = 0; i < n; i++) {
//            final long key = rand.nextLong();
//            final long value = rand.nextLong();
//            assertFalse(hashtable.containsKey(key));
//        }


        return new SingleResult(hashtable.size(), histogramPut, histogramPut, histogramPut);
    }

    private SingleResult benchmark(Long2LongHashMap hashtable, long[] keys) {

        final Histogram histogramPut = new Histogram(60_000_000_000L, 3);

        for (long key : keys) {
            long t = System.nanoTime();
            hashtable.put(key, key);
            long putNs = (System.nanoTime() - t);
            histogramPut.recordValue(putNs);
        }

//        t = System.nanoTime();
//        long acc = 0;
//        for (long key : keys) acc += hashtable.get(key);
//        long getNs = (System.nanoTime() - t) / keys.length;
//
//        t = System.nanoTime();
//        for (long key : keys) hashtable.remove(key);
//        long removNs = (System.nanoTime() - t) / keys.length;
//        return new SingleResult(hashtable.size(), putNs, getNs, removNs, acc);

        return new SingleResult(hashtable.size(), histogramPut, histogramPut, histogramPut);
    }


    private SingleResult benchmark(Map<Long, Long> hashtable, long[] keys) {

        int tps = 1_000_000;


        final Histogram histogramPut = new Histogram(60_000_000_000L, 3);

        final long picosPerCmd = (1024L * 1_000_000_000L) / tps;
        final long startTimeNs = System.nanoTime();

        long planneTimeOffsetPs = 0L;
        long lastKnownTimeOffsetPs = 0L;

        for (int i = 0; i < keys.length; i++) {
            final long key = keys[i];
            planneTimeOffsetPs += picosPerCmd;
            while (planneTimeOffsetPs > lastKnownTimeOffsetPs) {
                lastKnownTimeOffsetPs = (System.nanoTime() - startTimeNs) << 10;
                // spin until its time to send next command
                Thread.onSpinWait(); // 1us-26  max34
                // LockSupport.parkNanos(1L); // 1us-25 max29
                // Thread.yield();   // 1us-28  max32
            }
            hashtable.put(key, key);
            final long putNs = System.nanoTime() - startTimeNs - (lastKnownTimeOffsetPs >> 10);
            histogramPut.recordValue(putNs);
        }

        return new SingleResult(hashtable.size(), histogramPut, histogramPut, histogramPut);
    }


    record SingleResult(long size, Histogram avgPut, Histogram avgGet, Histogram avgRemove) {

    }


}
