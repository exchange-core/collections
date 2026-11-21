# Benchmarks

[exchange collections](../README.md) › Benchmarks

The numbers in the docs come from `tests-perf`. Each measurement names its benchmark and what it sets on its own
(the parameters, the heap); the machine and the settings they share are here.

## The machine

Two Xeon Gold 6244 (8 cores and 24.75 MB of L3 each), a NUMA node per socket: a random read of a 1 GB array takes
88 ns from the node of the thread and 146 ns from the other one ([NUMA](affinity.md#numa)). The measuring thread
pins itself to an isolated core through `collections-affinity`: one of `-Daffinity.reserved=<hex mask>`, otherwise
of the kernel's isolated CPUs. JDK 27, unless a measurement says otherwise.

## The queue benchmarks

All three queue benchmarks ran on a Xeon Gold 6244 (HT enabled), JDK 27+35,
JMH 1.34. The JVM started on two housekeeping CPUs of one socket with memory bound to its NUMA node,
`-XX:+AlwaysPreTouch` and `-XX:+UseTransparentHugePages`. With `-Daffinity.reserved` set to the isolated CPUs,
each benchmark thread pinned itself to an isolated core of that socket and reserved both of its hyper-threads.
All runs used `-t 1 -f 2 -wi 3 -w 500ms -i 5 -r 500ms`. Times are means with JMH's 99.9% confidence intervals.

The three are `LongIndexedFifoQueuesJmh`, `FifoOrderBookJmh` and `LongIndexedEventQueueJmh`
([queues](queues.md)); `LongSnapshotRecordTableJmh` ([records](records.md#longsnapshotrecordtable)) and
`LongPriceLadderJmh` ([price ladder](price-and-time.md#longpriceladder)) ran with the same settings.

## Running

Benchmarks are latency-sensitive: run them on an idle machine, ideally with isolated CPUs for the measuring threads.

Latency samples — `CollectionsLatencySamples` writes a CSV of samples per scenario into `-Dsamples.dir` (default
`tests-perf/target/samples-collections`); `-Dsamples.groups` picks the groups (`art-fill`, `hashtable-collide`,
`ll2-scenarios`, ...: the javadoc of the class lists them and the other properties):

```bash
mvn test -Pbenchmarks -pl tests-perf -am -Dsurefire.failIfNoSpecifiedTests=false \
    -Dtest=CollectionsLatencySamples -Dsamples.groups=art-fill
```

JMH — the `*Jmh` classes run with `org.openjdk.jmh.Main`, not as tests. Build the test classes with their
dependencies once, then run from `tests-perf/target`:

```bash
mvn -pl tests-perf -am -DskipTests package dependency:copy-dependencies -DincludeScope=test -DoutputDirectory=target/lib
cd tests-perf/target
java -cp "test-classes:lib/*" org.openjdk.jmh.Main LongPriceLadderJmh -rf json -rff out.json
```

With `-Daffinity.reserved` (or the kernel's isolated CPUs) the benchmark thread pins itself to one of them.
