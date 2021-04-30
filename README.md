# Bullet Core

[![Build Status](https://cd.screwdriver.cd/pipelines/7220/badge)](https://cd.screwdriver.cd/pipelines/7220)
[![Coverage Status](https://coveralls.io/repos/github/bullet-db/bullet-core/badge.svg?branch=master)](https://coveralls.io/github/bullet-db/bullet-core?branch=master) 
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.yahoo.bullet/bullet-core/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.yahoo.bullet/bullet-core/)

Bullet is a streaming query engine that can be plugged into any singular data stream using a Stream Processing framework like Apache [Storm](https://storm.apache.org), [Spark](https://spark.apache.org) or [Flink](https://flink.apache.org). It lets you run queries on this data stream - including hard queries like Count Distincts, Top K etc.

## Table of Contents

- [Background](#background)
- [Install](#install)
- [Usage](#usage)
- [Documentation](#documentation)
- [Links](#links)
    - [Quick Links](#quick-links)
- [Contributing](#contributing)
- [License](#license)

## Background

In Bullet, both the queries and the data flow through the system. There is absolutely no persistence layer! Queries live as long as their duration and operate on data in-memory only. So, the queries in Bullet look *forward* in time, which is pretty unique for most querying systems.

We created Bullet initially as a simple distributed grep like tool to find events in a click stream (containing high volume - 1 million events per sec -- user interaction data) at Yahoo. In particular, we use it for validating instrumentation that generates these events by interacting with the pages ourselves and finding our own events in this data stream and validate it for the proper key/value pairs. There was nothing as light-weight and cheap as Bullet to do this task.
There are many other use-cases for Bullet and indeed, how you use it, depends on your data stream. If you put Bullet on performance metric data, your queries might mostly be finding the 99th percentile of some latency metric etc.

This project is the core library for Bullet that lets us implement Bullet agnostically on any JVM based Stream Processor. See [Bullet Storm](https://github.com/bullet-db/bullet-storm), which uses this to implement Bullet on Storm and [Bullet Spark](https://github.com/bullet-db/bullet-spark), on Spark Streaming. This code initially lived inside the [Bullet Storm](https://github.com/bullet-db/bullet-storm) code base up to Bullet Storm Version [0.4.3](https://github.com/bullet-db/bullet-storm/releases/tag/bullet-storm-0.4.3).

## Install

Bullet Core is a library written in Java and published to [Bintray](https://bintray.com/yahoo/maven/bullet-core) and mirrored to [JCenter](http://jcenter.bintray.com/com/yahoo/bullet/bullet-core/).
It is meant to be used to implement Bullet on different Stream Processors or to implement a Bullet [PubSub](https://bullet-db.github.io/pubsub/architecture/). To see the various versions and set up your project for your package manager (Maven, Gradle etc), [see here](https://bullet-db.github.io/releases/#bullet-core).

## Usage

Once you have added a dependency for Bullet Core, use our abstractions for the PubSub, Parsing, Querying, Windowing, Partitioning, and Sketching as you need to. In particular, see how we abstract running a [Bullet Query](https://github.com/bullet-db/bullet-core/blob/master/src/main/java/com/yahoo/bullet/querying/Querier.java). You can also look at our reference implementations in [Storm](https://github.com/bullet-db/bullet-storm) and [Spark](https://github.com/bullet-db/bullet-spark) to get a better idea.

## Documentation

All documentation is available at **[Github Pages here](https://bullet-db.github.io)**.

## Links

* [Main Documentation](https://bullet-db.github.io/) to see documentation.
* [Bullet Core Releases](https://bullet-db.github.io/releases/#bullet-core) to see Bullet Core releases.

### Quick Links

* [Spark Quick Start](https://bullet-db.github.io/quick-start/spark) to start with a Bullet instance running locally on Spark.
* [Storm Quick Start](https://bullet-db.github.io/quick-start/storm) to start with a Bullet instance running locally on Storm.
* [Spark Architecture](https://bullet-db.github.io/backend/spark-architecture/) to see how Bullet is implemented on Storm.
* [Storm Architecture](https://bullet-db.github.io/backend/storm-architecture/) to see how Bullet is implemented on Storm.
* [Setup on Spark](https://bullet-db.github.io/backend/spark-setup/) to see how to setup Bullet on Spark.
* [Setup on Storm](https://bullet-db.github.io/backend/storm-setup/) to see how to setup Bullet on Storm.
* [API Examples](https://bullet-db.github.io/ws/examples/) to see what kind of queries you can run on Bullet.
* [Setup Web Service](https://bullet-db.github.io/ws/setup/) to setup the Bullet Web Service.
* [Setup UI](https://bullet-db.github.io/ui/setup/) to setup the Bullet UI.

## Contributing

All contributions are welcomed! Feel free to submit PRs for bug fixes, improvements or anything else you like! Submit issues, ask questions using Github issues as normal and we will classify it accordingly. See [Contributing](Contributing.md) for a more in-depth policy. We just ask you to respect our [Code of Conduct](Code-of-Conduct.md) while you're here.

## License

Code licensed under the Apache 2 license. See the [LICENSE](LICENSE) for terms.
