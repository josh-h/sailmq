## Apache SailMQ

[![Build Status][maven-build-image]][maven-build-url]
[![CodeCov][codecov-image]][codecov-url]
[![Maven Central][maven-central-image]][maven-central-url]
[![Release][release-image]][release-url]
[![License][license-image]][license-url]
[![Average Time to Resolve An Issue][percentage-of-issues-still-open-image]][percentage-of-issues-still-open-url]
[![Percentage of Issues Still Open][average-time-to-resolve-an-issue-image]][average-time-to-resolve-an-issue-url]
[![Twitter Follow][twitter-follow-image]][twitter-follow-url]

**[Apache SailMQ](https://mq.sail.org) is a distributed messaging and streaming platform with low latency, high performance and reliability, trillion-level capacity and flexible scalability.**


It offers a variety of features:

* Messaging patterns including publish/subscribe, request/reply and streaming
* Financial grade transactional message
* Built-in fault tolerance and high availability configuration options base on [DLedger Controller](docs/en/controller/quick_start.md)
* Built-in message tracing capability, also support opentracing
* Versatile big-data and streaming ecosystem integration
* Message retroactivity by time or offset
* Reliable FIFO and strict ordered messaging in the same queue
* Efficient pull and push consumption model
* Million-level message accumulation capacity in a single queue
* Multiple messaging protocols like gRPC, MQTT, JMS and OpenMessaging
* Flexible distributed scale-out deployment architecture
* Lightning-fast batch message exchange system
* Various message filter mechanics such as SQL and Tag
* Docker images for isolated testing and cloud isolated clusters
* Feature-rich administrative dashboard for configuration, metrics and monitoring
* Authentication and authorization
* Free open source connectors, for both sources and sinks
* Lightweight real-time computing
----------


## Quick Start

This paragraph guides you through steps of installing SailMQ in different ways.
For local development and testing, only one instance will be created for each component.

### Run SailMQ locally

SailMQ runs on all major operating systems and requires only a Java JDK version 8 or higher to be installed.
To check, run `java -version`:
```shell
$ java -version
java version "1.8.0_121"
```

For Windows users, click [here](https://dist.apache.org/repos/dist/release/sailmq/5.2.0/sailmq-all-5.2.0-bin-release.zip) to download the 5.2.0 SailMQ binary release,
unpack it to your local disk, such as `D:\sailmq`.
For macOS and Linux users, execute following commands:

```shell
# Download release from the Apache mirror
$ wget https://dist.apache.org/repos/dist/release/sailmq/5.2.0/sailmq-all-5.2.0-bin-release.zip

# Unpack the release
$ unzip sailmq-all-5.2.0-bin-release.zip
```

Prepare a terminal and change to the extracted `bin` directory:
```shell
$ cd sailmq-all-5.2.0-bin-release/bin
```

**1) Start NameServer**

NameServer will be listening at `0.0.0.0:9876`, make sure that the port is not used by others on the local machine, and then do as follows.

For macOS and Linux users:
```shell
### start Name Server
$ nohup sh mqnamesrv &

### check whether Name Server is successfully started
$ tail -f ~/logs/sailmqlogs/namesrv.log
The Name Server boot success...
```

For Windows users, you need set environment variables first:
- From the desktop, right click the Computer icon.
- Choose Properties from the context menu.
- Click the Advanced system settings link.
- Click Environment Variables.
- Add Environment `ROCKETMQ_HOME="D:\sailmq"`. 

Then change directory to sailmq, type and run:
```shell
$ mqnamesrv.cmd
The Name Server boot success...
```

**2) Start Broker**

For macOS and Linux users:
```shell
### start Broker
$ nohup sh bin/mqbroker -n localhost:9876 &

### check whether Broker is successfully started, eg: Broker's IP is 192.168.1.2, Broker's name is broker-a
$ tail -f ~/logs/sailmqlogs/broker.log
The broker[broker-a, 192.169.1.2:10911] boot success...
```

For Windows users:
```shell
$ mqbroker.cmd -n localhost:9876
The broker[broker-a, 192.169.1.2:10911] boot success...
```

### Run SailMQ in Docker

You can run SailMQ on your own machine within Docker containers,
`host` network will be used to expose listening port in the container.

**1) Start NameServer**

```shell
$ docker run -it --net=host apache/sailmq ./mqnamesrv
```

**2) Start Broker**

```shell
$ docker run -it --net=host --mount source=/tmp/store,target=/home/sailmq/store apache/sailmq ./mqbroker -n localhost:9876
```

### Run SailMQ in Kubernetes

You can also run a SailMQ cluster within a Kubernetes cluster using [SailMQ Operator](https://github.com/sail/sailmq-operator).
Before your operations, make sure that `kubectl` and related kubeconfig file installed on your machine.

**1) Install CRDs**
```shell
### install CRDs
$ git clone https://github.com/sail/sailmq-operator
$ cd sailmq-operator && make deploy

### check whether CRDs is successfully installed
$ kubectl get crd | grep sailmq.apache.org
brokers.sailmq.apache.org                 2022-05-12T09:23:18Z
consoles.sailmq.apache.org                2022-05-12T09:23:19Z
nameservices.sailmq.apache.org            2022-05-12T09:23:18Z
topictransfers.sailmq.apache.org          2022-05-12T09:23:19Z

### check whether operator is running
$ kubectl get pods | grep sailmq-operator
sailmq-operator-6f65c77c49-8hwmj   1/1     Running   0          93s
```

**2) Create Cluster Instance**
```shell
### create SailMQ cluster resource
$ cd example && kubectl create -f sailmq_v1alpha1_sailmq_cluster.yaml

### check whether cluster resources is running
$ kubectl get sts
NAME                 READY   AGE
broker-0-master      1/1     107m
broker-0-replica-1   1/1     107m
name-service         1/1     107m
```

---
## Apache SailMQ Community
* [SailMQ Streams](https://github.com/sail/sailmq-streams): A lightweight stream computing engine based on Apache SailMQ.
* [SailMQ Flink](https://github.com/sail/sailmq-flink): The Apache SailMQ connector of Apache Flink that supports source and sink connector in data stream and Table.
* [SailMQ APIs](https://github.com/sail/sailmq-apis): SailMQ protobuf protocol.
* [SailMQ Clients](https://github.com/sail/sailmq-clients): gRPC/protobuf-based SailMQ clients.
* SailMQ Remoting-based Clients
	 - [SailMQ Client CPP](https://github.com/sail/sailmq-client-cpp)
	 - [SailMQ Client Go](https://github.com/sail/sailmq-client-go)
	 - [SailMQ Client Python](https://github.com/sail/sailmq-client-python)
	 - [SailMQ Client Nodejs](https://github.com/sail/sailmq-client-nodejs)
* [SailMQ Spring](https://github.com/sail/sailmq-spring): A project which helps developers quickly integrate Apache SailMQ with Spring Boot.
* [SailMQ Exporter](https://github.com/sail/sailmq-exporter): An Apache SailMQ exporter for Prometheus.
* [SailMQ Operator](https://github.com/sail/sailmq-operator): Providing a way to run an Apache SailMQ cluster on Kubernetes.
* [SailMQ Docker](https://github.com/sail/sailmq-docker): The Git repo of the Docker Image for Apache SailMQ.
* [SailMQ Dashboard](https://github.com/sail/sailmq-dashboard): Operation and maintenance console of Apache SailMQ.
* [SailMQ Connect](https://github.com/sail/sailmq-connect): A tool for scalably and reliably streaming data between Apache SailMQ and other systems.
* [SailMQ MQTT](https://github.com/sail/sailmq-mqtt): A new MQTT protocol architecture model, based on which Apache SailMQ can better support messages from terminals such as IoT devices and Mobile APP.
* [SailMQ EventBridge](https://github.com/sail/sailmq-eventbridge): EventBridge make it easier to build a event-driven application.
* [SailMQ Incubating Community Projects](https://github.com/sail/sailmq-externals): Incubator community projects of Apache SailMQ, including [logappender](https://github.com/sail/sailmq-externals/tree/master/logappender), [sailmq-ansible](https://github.com/sail/sailmq-externals/tree/master/sailmq-ansible), [sailmq-beats-integration](https://github.com/sail/sailmq-externals/tree/master/sailmq-beats-integration), [sailmq-cloudevents-binding](https://github.com/sail/sailmq-externals/tree/master/sailmq-cloudevents-binding), etc.
* [SailMQ Site](https://github.com/sail/sailmq-site): The repository for Apache SailMQ website.
* [SailMQ E2E](https://github.com/sail/sailmq-e2e): A project for testing Apache SailMQ, including end-to-end, performance, compatibility tests.


----------
## Learn it & Contact us
* Mailing Lists: <https://mq.sail.org/about/contact/>
* Home: <https://mq.sail.org>
* Docs: <https://mq.sail.org/docs/quick-start/>
* Issues: <https://github.com/sail/sailmq/issues>
* Rips: <https://github.com/sail/sailmq/wiki/SailMQ-Improvement-Proposal>
* Ask: <https://stackoverflow.com/questions/tagged/sailmq>
* Slack: <https://sailmq-invite-automation.herokuapp.com/>


----------



## Contributing
We always welcome new contributions, whether for trivial cleanups, [big new features](https://github.com/sail/sailmq/wiki/SailMQ-Improvement-Proposal) or other material rewards, more details see [here](http://mq.sail.org/docs/how-to-contribute/).

----------
## License
[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html) Copyright (C) Apache Software Foundation


----------
## Export Control Notice
This distribution includes cryptographic software. The country in which you currently reside may have
restrictions on the import, possession, use, and/or re-export to another country, of encryption software.
BEFORE using any encryption software, please check your country's laws, regulations and policies concerning
the import, possession, or use, and re-export of encryption software, to see if this is permitted. See
<http://www.wassenaar.org/> for more information.

The U.S. Government Department of Commerce, Bureau of Industry and Security (BIS), has classified this
software as Export Commodity Control Number (ECCN) 5D002.C.1, which includes information security software
using or performing cryptographic functions with asymmetric algorithms. The form and manner of this Apache
Software Foundation distribution makes it eligible for export under the License Exception ENC Technology
Software Unrestricted (TSU) exception (see the BIS Export Administration Regulations, Section 740.13) for
both object code and source code.

The following provides more details on the included cryptographic software:

This software uses Apache Commons Crypto (https://commons.apache.org/proper/commons-crypto/) to
support authentication, and encryption and decryption of data sent across the network between
services.

[maven-build-image]: https://github.com/sail/sailmq/actions/workflows/maven.yaml/badge.svg
[maven-build-url]: https://github.com/sail/sailmq/actions/workflows/maven.yaml
[codecov-image]: https://codecov.io/gh/sail/sailmq/branch/master/graph/badge.svg
[codecov-url]: https://codecov.io/gh/sail/sailmq
[maven-central-image]: https://maven-badges.herokuapp.com/maven-central/org.sail.mq/sailmq-all/badge.svg
[maven-central-url]: http://search.maven.org/#search%7Cga%7C1%7Corg.sail.mq
[release-image]: https://img.shields.io/badge/release-download-orange.svg
[release-url]: https://www.apache.org/licenses/LICENSE-2.0.html
[license-image]: https://img.shields.io/badge/license-Apache%202-4EB1BA.svg
[license-url]: https://www.apache.org/licenses/LICENSE-2.0.html
[average-time-to-resolve-an-issue-image]: http://isitmaintained.com/badge/resolution/sail/sailmq.svg
[average-time-to-resolve-an-issue-url]: http://isitmaintained.com/project/sail/sailmq
[percentage-of-issues-still-open-image]: http://isitmaintained.com/badge/open/sail/sailmq.svg
[percentage-of-issues-still-open-url]: http://isitmaintained.com/project/sail/sailmq
[twitter-follow-image]: https://img.shields.io/twitter/follow/ApacheSailMQ?style=social
[twitter-follow-url]: https://twitter.com/intent/follow?screen_name=ApacheSailMQ
