# Thread affinity

[exchange collections](../README.md) › Thread affinity

`collections-affinity` pins a thread to one CPU and reserves its whole physical core (hyper-threading
siblings included) — within the process and, through lock files, across the processes that share the lock directory.
Linux (`sched_setaffinity`) and Windows (`SetThreadAffinityMask`, processor group 0); elsewhere the handle
reports that nothing was pinned. It calls the OS through the Foreign Function and Memory API: Java 22+, no
native library, and it also works in a GraalVM native image.

```java
import exchange.core2.collections.affinity.CpuAffinity;

try (CpuAffinity affinity = CpuAffinity.acquireCore()) {   // or acquireCore(cpu)
    // hot loop on affinity.cpu()
}
```

- `acquireCore()` takes a free core from the reserved CPUs: `-Daffinity.reserved=<hex mask>`
  (e.g. `AAAA` = CPUs 1,3,…,15), otherwise the kernel's isolated CPUs (`isolcpus`). The highest CPU first, one
  NUMA node before the next. If none is free it returns a handle that is not pinned (`isPinned()`), it never
  throws. A malformed value of an `-Daffinity.*` property is logged as a warning and ignored, as if it was not set:
  a list such as `5,7,9` where a mask belongs among them.
- `acquireCore(int cpu)` pins to that CPU and throws `IllegalStateException` if its core is held.
- `acquireCpu(int cpu)` pins to that logical CPU and reserves only it, so that two threads can share a core
  (e.g. `acquireCpu(5)` and `acquireCpu(21)`). It throws `IllegalStateException` if the CPU is held;
  `acquireCore` does not take a core while one of its CPUs is held.
- `close()` restores the thread's previous mask and releases what it reserved — call it from the pinned thread.
- Run with `--enable-native-access=ALL-UNNAMED` to avoid the JDK's restricted-method warning.
- Lock files: `exchange-core2-cpu-<n>.lock` in `-Daffinity.lockDir` (default `java.io.tmpdir`, created if
  missing). Only processes with the same directory see each other's cores: a systemd service with `PrivateTmp` has a
  `/tmp` of its own, and on Windows each user has a temp directory of their own. A CPU whose lock file can not be
  used — a symbolic link, which is not followed, or a file another user can not open — is not taken: the warning
  names the file, and `acquireCore()` goes on to the next core.
- A virtual thread is not pinned: the OS sees only its carrier, which runs other virtual threads next. Pin a
  platform thread, such as those of `CpuAffinity.threadFactory`.
- Threads started by a pinned thread inherit its single-CPU mask: start helper threads before pinning, or
  let them pin themselves - e.g. `MigrationService.builder().threadInit(() -> CpuAffinity.acquireCore(5))`.

## NUMA

On a machine of several sockets it matters where a thread runs and where its memory is. On two Xeon Gold 6244
a random read of a 1 GB array takes 88 ns from the node of the thread and 146 ns from the other one, and a write
reaches a thread of the same socket in 90-124 ns, of the other socket in ~290 ns.

```java
NumaTopology topology = NumaTopology.current();    // from sysfs, no native access needed
topology.cpusOf(1);                                // the CPUs of node 1
topology.distance(0, 1);                           // 21
topology.lastLevelCacheOf(13);                     // less than the node on many AMD CPUs
topology.nodeOfNetworkInterface("eno1np0");        // or nodeOfPciDevice("0000:18:00.0")

try (CpuAffinity hot = CpuAffinity.acquireCoreOnNode(1)) {    // a reserved core of that node
    // on another thread: CpuAffinity.acquireCoreNear(hot) - same cache, then same node, then by distance
    long[] table = new long[1 << 27];
    Arrays.fill(table, 1);
    NumaMemory.nodesOf(MemorySegment.ofArray(table));         // node 1 100% of 4096 sampled pages
}
```

- `acquireCoreOnNode` and `acquireCoreNear` never throw for lack of a core, the handle is then not pinned;
  `node()` tells where a handle landed. Without NUMA, or off Linux, the machine is one node 0.
- Where only one socket is isolated, `-Daffinity.spare=<hex mask>` allows CPUs of the other one, which are not
  taken otherwise: they may serve interrupts. Each node gives its reserved CPUs before its spare ones.
  `acquireCore()` fills one node and goes on to the next by distance. It starts on `-Daffinity.firstNode=<node>`, by
  default the node of the highest reserved CPU, and the first landing on another node is logged as a warning.
  A group of threads can go to its node from the start, without taking the cores of the first one:

  ```java
  // -Daffinity.spare=5554: on top of isolcpus 5-15 (odd), the CPUs 2,4,..,14 of node 0
  try (CpuAffinity hot = CpuAffinity.acquireCore()) { /* node 1 while it has cores, then node 0 */ }
  ExecutorService background = Executors.newFixedThreadPool(2, CpuAffinity.threadFactory("bg", 0));   // bg-0, bg-1
  ```

  A factory's thread pins itself when it starts and releases the core when it ends. The cores of a group are not set
  aside: threads overflowing from node 1 can take them, so start the group first if they may run short.
- The JVM can not be moved from inside: it sizes its thread pools from the CPU mask it starts with (`taskset -c 0-31`
  gives 23 GC threads and 15 compiler threads, `1,3` two of each) and its memory goes by the policy it starts with.
  So the first thread on the node filled first logs a warning for each thing that stands in the way, once:
  the node has no memory, the policy or the cpuset sends the memory elsewhere, there is no policy and the JVM started on
  CPUs of other nodes (148 ns instead of 89 ns on the machine above), the memory already is elsewhere, the heap limit
  plus `-Daffinity.offHeap=<size>` does not fit. Threads on other nodes hear only if their node has no memory.

  ```java
  ProcessPlacement placement = ProcessPlacement.current();   // from /proc/self, no native access needed
  placement.policy();                                        // "bind:1", or "default" without numactl
  placement.bytesOn(1);                                      // the heap and native memory there now
  placement.warningsFor(1);                                  // the same warnings, to fail at start-up
  NumaTopology.current().memoryOf(1).free();                 // bytes, also total(), reclaimable(), freeHugePages()
  ```

  The free memory of a node is an estimate: there is no `MemAvailable` per node. Its reserved huge pages are reported
  separately: they require explicit mappings of the matching page size and cannot satisfy ordinary allocations,
  even if the process already has a huge mapping. Resident huge pages and the memfd heap of ZGC count with the
  process's memory. It does not tell the memory channels apart: on the machine above node 0 has 2, node 1 has 6,
  and only their sizes differ. `ProcessPlacement` walks the page
  tables, so at start-up or between runs: with a heap of 16 GiB about 2 ms in huge pages, 65 ms in 4 KB pages.
- `NumaMemory.nodesOf` only reports, it moves nothing, and it is an estimate: up to 4096 random pages, one from
  each equal part of the segment, a few milliseconds, not for a hot path. Native memory and arrays, Linux on
  x86-64 and AArch64. `isSupported()` tried the system call: the default seccomp profile of Docker refuses it.
  `absentPages()` are the pages not touched yet. An array is asked about again if the GC moved it during the call,
  and the answer is for that moment: a large array usually stays where it is, a small one moves with every GC.
- It does not place memory. For the heap the reliable way is the whole JVM on one node, `numactl --membind=1`
  (any JVM). Without it a page goes where the thread touching it first runs: the GC threads with
  `-XX:+AlwaysPreTouch`, and reused regions stay on the node they were first touched from. `-XX:+UseNUMA` with
  G1 spreads a large array over the nodes, 50/50 on two (JEP 345 leaves humongous regions out).
- Where the JVM itself starts is a trade-off for a thread whose data is in the L3. On the machine above (JDK 27, G1,
  the heap on node 1, two GC threads, a thread on node 1 chasing 16 MB), with the JVM on node 1 (`taskset -c 1,3`)
  a pause copying objects scattered over the heap took 247 ms instead of 316 ms on node 0 (`2,4`), but it pushed the
  thread's data out of the L3, and for 60 ms after it the thread ran at DRAM speed. With the JVM on node 0 no slowdown
  was seen after a pause. Copying objects in order the node made no difference; pauses with little to copy took
  0.5 ms on node 1, 0.8 ms on node 0. So keep the JVM off that L3 and bind only its memory:
  `numactl --membind=1 taskset -c 2,4`. On CPUs 1,3, `nohz_full` there, the two GC threads at times ran one after
  the other, and those pauses were twice as long: `-Xlog:gc+cpu` shows `User` close to `Real`.
- The arrays of a [`LongLongLL2Hashtable`](hashtables.md#longlongll2hashtable) are first touched by different threads: the table's thread creates the
  first one and any the service is too late with, the worker of its `MigrationService` the new ones up to 4 MB, the
  allocator the larger ones. A table
  stays on a node only if all of them run there - `threadInit` and `allocatorThreadInit` with
  `CpuAffinity.acquireCoreOnNode(1)` - so tables on several nodes need a service per node. Even then reused heap
  regions stay on their old node: verify with `nodesOf`.
