---
title: 000 - File Data Ingestion
description: Unified API for reading files and improved packaging for
             cloud sources
---

*Since 4.4.*

= 2. Support for common formats and data sources

Format and source of the data are seemingly orthogonal. You can read
plain text files/csv/avro/.. etc from a local filesystem, S3 etc.

However, to take an advantage of some properties of certain formats
(e.g. parallel reading/deserialization of Avro files, selecting columns
in parquet …) requires combining these two steps (read Avro file header
to find out the beginning positions of data blocks).

This is already implemented in the Hadoop connector.

Required Formats

* CSV
We don’t have a connector
There is not official Hadoop connector (= InputFormat), open-source
implementations exist, with limitations (same as with Spark)

* JSON
We have a connector
There is not official Hadoop connector (= InputFormat), open-source
implementations exist

* Avro
We have a connector
There is official Hadoop connector

* Parquet
We don’t have a connector
There is official Hadoop connector

* ORC low priority

** [Reading ORC files](https://orc.apache.org/docs/core-java.html#reading-orc-files)

== Parquet

Parquet is a special case between the formats listed above - it defines
representation in the file (as others do), but doesn't have own schema
(class?) definition for deserialization into objects. Uses one of the
other serialization formats, most commonly thrift, avro.

The PRD doesn't specify the following commonly used formats:

* plain text,

* binary (e.g. images for ML)

* Protobuf

* Thrift

Sources

* local filesystem

* Amazon S3

* Azure Blob Storage

* Azure Data Lake Storage

* Google Cloud Storage.

* HDFS ? not listed in the PRD

== Unified approach for file data ingestion

We have the following possibilities

1. enforce naming convention, e.g.
`com.hazelcast.jet.x.XSources.x(...., formatY)`
`com.hazelcast.jet.s3.S3Sources.s3(..., Avro)`

2. new File API as a single entry point e.g.
`FileSources.s3(&quot;...&quot;).withFormat(AVRO)`

3. URL/ resource description style with auto detection, e.g.
`FileSource.read(&quot;s3://...../file.avro&quot;);`

We think we should go with 2., potentially write a parser for 3. to
delegate to 2. - would be used from SQL (there already is similar logic)

We decided to use Hadoop libraries to access all systems apart from
local filesystem, and Hadoop can detect the source we implemented 3.

== Using Hadoop libraries

Using Hadoop libraries is a must for reading data from HDFS.

To read data from other sources, like S3 it is possible to use custom
connectors (e.g. we have a connector for S3). Both approaches have some
advantages and disadvantages.

Using Hadoop libs

* `-` Complicated to setup, lot’s of dependencies, possible dependency
  conflicts.

* `+` Supports advanced features - can take advantage of the structure
  of the formats - e.g. read avro file in parallel, or read specific
columns in parquet.  +-? The hadoop api leaks (see
`com.hazelcast.jet.hadoop.HadoopSources#inputFormat(org.apache.hadoop.conf.Configuration,
com.hazelcast.function.BiFunctionEx<K,V,E>`) It is questionable if this
is an issue, exposing the hadoop API gives users more power.  For
performance difference see the benchmark below

Using specific s3/... connector

* `-` we would need to implement a connector for each source (currently we
    have local filesystem and S3), the S3 source connector is ~300 lines

* `-` we would miss the advanced features, reimplementing those would be
  a huge effort

* `+` Simpler packaging

* `+` Nicer API

* `+-`? Unknown performance compared to Hadoop libs

Benchmark S3 client based vs Hadoop based connectors

Benchmark summary: S3 is generally faster, apart from 1 large file case,
where Hadoop can split the file. The difference is not massive though
(18211 fastest for s3, vs 21306 for Hadoop, slowest 75752 for S3, 90071
for Hadoop)

We decided to use Hadoop to access all sources, with option without
hadoop for local filesystems

= 1. Loading data from a file Cookbook

There is a section in the manual describing the new API and each module.

There are examples how to read:

* binary files
* text files by lines
* avro files

= 3. Any other concerns?

Compression

Hadoop supports various compression formats.

= 4. Overlap with Jet SQL

The main difference in how SQL would use the connector is that it
expects `Object[]` as return type.

We need to provide a way to configure each format as such.

= Licensing

We had to add couple of aliases for apache 2, BSD, new/revised BSD,
nothing new

What's new is:

* "The Go license" - this is permissive BSD style license,
[link](https://golang.org/LICENSE)

* CDDL (1.0) - Common Development and Distribution License - this is
a weak copyleft license, based on mozilla public license and its
variants

** CDDL 1.1 (minor update, something with patents and EU law)
** CDDL + GPLv2 with classpath exception - this is just dual CDDL
1.0 + GPLv2 license

All the CDDL libs are transitive dependencies of hadoop-common, which
we plan to package in the all deps included file connectors jars for
s3/gcp/azure.

Many commercial applications use CDDL dependencies (e.g. Spring
framework has many modules with transitive dependencies under CDDL)
