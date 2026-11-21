# Contributing

Exchange Collections is an open-source project and contributions are welcome!

The requirements and the build: [README](README.md#requirements).

## Documentation

The [README](README.md) is the entry point: the modules, the installation and a table of the collections. The
collections themselves are described in `docs/`, a file per family (hashtables, records, queues, ...), a section
per class: an example, the features, the cost of the operations, measurements and notes. A new collection gets a
section in the file of its family and a row in the table of the README.

## Testing

Unit tests, including property-based tests and an oracle that compares ART against `TreeMap`:

```bash
mvn test
```

Long-running randomized stress tests (`tests-stress`) and latency / throughput benchmarks
(`tests-perf`) live in separate modules and are left out of a plain build — run them with their
profiles (`-Dtest=<class>` for one class of benchmarks):

```bash
mvn -Pstress -pl tests-stress -am test
```

```bash
mvn -Pbenchmarks -pl tests-perf -am test
```

Benchmarks are latency-sensitive: run them on an idle machine, ideally with CPU isolation — the
measuring threads pin themselves to the isolated CPUs through [`collections-affinity`](docs/affinity.md).
How the numbers in the docs were measured, and how to run one benchmark or JMH: [benchmarks](docs/benchmarks.md).

The API of the published modules against the last release ([japicmp](https://github.com/siom79/japicmp)):
a binary or source incompatible change fails the build unless it is listed in the excludes of the
`api-check` profile; the report goes to `target/japicmp`.

```bash
mvn -Papi-check -DskipTests verify
```

The javadoc is strict: a broken reference or bad HTML fails the build.
