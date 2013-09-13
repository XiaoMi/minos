# Minos Deployment Client

This is the client for Minos.  You can use this client to perform various deployment tasks, e.g. installing, (re)starting, stopping a service.  Currently, this client supports ZooKeeper, HDFS, HBase, YARN and Impala.  It can be extended to support other systems.

## Prerequisites

In order to run the deployment client, you need the following prerequisites:

    1. python 2.7 or later from <http://www.python.org>
    2. pexpect (latest) from <http://www.noah.org/wiki/pexpect>
    3. JDK 1.6 from <http://www.oracle.com/technetwork/java/javase/downloads/index.html>
    4. ConfigObj 4 from <http://www.voidspace.org.uk/python/configobj.html>

## A Simple Tutorial

Here we would like to show you how to use the client in a simple tutorial.  In this tutorial we will use Minos to deploy an HDFS service, which itself requires the deployment of a ZooKeeper service.

The following are some conventions we will use in this tutorial:

* **Cluster type**: we define three types of clusters: `tst` for testing, `prc` for offline processing, and `srv` for online serving.
* **ZooKeeper cluster name**: we define the ZooKeeper cluster name using the IDC short name and the cluster type.  For example, `dptst` is used to name a testing cluster at IDC `dp`.
* **Other service cluster names**: we define other service cluster names using the corresponding ZooKeeper cluster name and the name of the business for which the service is intended to serve.  For example, the `dptst-example` is the name of a testing cluster used to do example tests.
* **Configuration file names**: all the services will have a corresponding configuration file, which will be named as `${service}-${cluster}.cfg`.  For example, the `dptst` ZooKeeper service's configuration file is named as `zookeeper-dptst.cfg`, and the `dptst` example HDFS service's configuration file is named as `hdfs-dptst-example.cfg`.

### Setting Up Tank

Tank, the package server, is required by Minos.  When setting up a cluster for the first time, you should set up a tank server first.  This only needs to be done once.

You can refer to file `README.md` under the `tank` directory to deploy the tank server.

### Deploying Supervisor

Supervisor, the process management server, is also required by our deployment system.  When setting up a cluster for the first time, you need to set up `supervisord` on every machine.  This also needs to be done only once.

You can refer to `README.md` under the `supervisor` directory to deploy `supervisord`.

### Configuring `deploy.cfg`

There is a configuration file named `deploy.cfg` under the root directory of this project.  You should first edit this file to set up the deployment environment.  Make sure that all service packages are prepared and configured in `deploy.cfg`.

### Configuring ZooKeeper

As mentioned in the cluster naming conventions, we will set up a testing ZooKeeper cluster at the `dp` IDC, and the corresponding configuration file for the cluster will be named as `zookeeper-dptst.cfg`.

You can edit `zookeeper-dptst.cfg` under the `config/conf/zookeeper` directory to configure the cluster.  The `zookeeper-dptst.cfg` is well commented and self explained, so we will not explain more here.

### Setting up a ZooKeeper Cluster

To set up a ZooKeeper cluster, just do the following two steps:

* Install a ZooKeeper package to the tank server:

        ./deploy.py install zookeeper dptst

* Bootstrap the cluster, this is only needed once when the cluster is setup for the first time:

        ./deploy.py bootstrap zookeeper dptst

Here are some handy ways to manage the cluster:

* Show the status of the ZooKeeper service:

        ./deploy.py show zookeeper dptst

* Start/Stop/Restart the ZooKeeper cluster:

        ./deploy.py stop zookeeper dptst
        ./deploy.py start zookeeper dptst
        ./deploy.py restart zookeeper dptst

* Clean up the ZooKeeper cluster:

        ./deploy.py cleanup zookeeper dptst

* Rolling update the ZooKeeper cluster:

        ./deploy.py rolling_update zookeeper dptst

### Configuring HDFS

Now it is time to configure the HDFS system.  Here we set up a testing HDFS cluster named `dptst-example`, whose configuration file will be named as `hdfs-dptst-example.cfg`, as explained in the naming conventions.

You can edit `hdfs-dptst-example.cfg` under the `config/conf/hdfs` directory to configure the cluster.  The `hdfs-dptst-example.cfg` is well commented and self explained, so we will net explain more here.

### Setting Up HDFS Cluster

Setting up and managing an HDFS cluster is similar to setting up and managing a ZooKeeper cluster.  The only difference is the cluster name, `dptst-example`, which implies that the corresponding ZooKeeper cluster is `dptst`:

    ./deploy.py install hdfs dptst-example
    ./deploy.py bootstrap hdfs dptst-example
    ./deploy.py show hdfs dptst-example
    ./deploy.py stop hdfs dptst-example
    ./deploy.py start hdfs dptst-example
    ./deploy.py restart hdfs dptst-example
    ./deploy.py rolling_update hdfs dptst-example --job=datanode
    ./deploy.py cleanup hdfs dptst-example

### Shell

The client tool also supports a very handy command named `shell`.  You can use this command to manage the files on HDFS, tables on HBase, jobs on YARN, etc.  Here are some examples about how to use the `shell` command to perform several different HDFS operations:

    ./deploy.py shell hdfs dptst-example dfs -ls /
    ./deploy.py shell hdfs dptst-example dfs -mkdir /test
    ./deploy.py shell hdfs dptst-example dfs -rm -R /test

You can run `./deploy.py --help` to see the detailed help messages.
