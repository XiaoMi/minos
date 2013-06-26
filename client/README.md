# The Minos Deploy Client
This is the client for the Minos deploy system, users will use this client
to perform the deployment works. Currently, this client support deploying
zookeeper, hdfs, hbase, yarn and impala. And it's very easy to extend
this client to support deploying other services.

## Prerequisites
In order to run the deploy client, we need the following prerequisites:

    1. python 2.6 or later from <http://www.python.org>
    2. pexpect (latest) from <http://www.noah.org/wiki/pexpect>
    3. JDK 1.6 from <http://www.oracle.com/technetwork/java/javase/downloads/index.html>

## A Simple Tutorial
Here we will give a simple example of deploying a hdfs service to show how
to use this deploy client.

As we all known, an hdfs service requires a zookeeper service, so we need to
deploy a zookeeper service at first.

Before starting to deploy the zookeeper and hdfs services, we will define
some conventions:

* **The cluster type**: we define three type of clusters, tst for testing, prc  
for offline processing and srv for online serving
* **The zookeeper cluster name**: we define the zookeeper cluster name use the  
IDC short name and the cluster type, such as dptst stands for testing cluster  
at dp IDC.
* **Other services cluster name**: we define other services cluster name use  
the corresponding zookeeper cluster name and the name of the business for which  
the service is intended to serve. For example, the dptst-example cluster stands  
for a testing cluster used to do example tests.
* **Config file name**: all the services will have a corresponding config files,  
the name of a specified service config file will be ${service}-${cluster}.cfg,  
for example, the dptst zookeeper service's config file will be named  
zookeeper-dptst.cfg, and the dptst example hdfs service's config file will be  
named hdfs-dptst-example.cfg.

### Deploy Tank
Tank, the package server, is required by our deploy system. So when a cluster
is setup for the first time, users should setup a tank server first, this only
needs to do once.

Users can refer to the README.md under the tank directory to deploy the tank
server.

### Deploy Supervisor
Supervisor, the process management server, is also required by our deploy
system. So when a cluster is setup for the first time, users should also setup
a supervisord on every machine, this only needs to do once.

Users can refer to the README.md under the supervisor directory to deploy the
supervisord.

### Config the deploy.cfg
There is a config file named deploy.cfg under the root directory of this
project, users should first edit this file to config their own deploy
environment. Here donot forget to prepare the service packages and to config
it in deploy.cfg.

### Config Zookeeper
As we have already introduced the cluster naming conventions, we will setup a
testing zookeeper cluster at dp IDC, so the corresponding config for the
cluster will named zookeeper-dptst.cfg.

Users can edit zookeeper-dptst.cfg under the config/conf/zookeeper directory
to config the cluster. The zookeeper-dptst.cfg is well commented and self
explained, so we will not explain it here.

### Setup a Zookeeper Cluster
To setup a zookeeper cluster, users need just do the following steps:

* Install a zookeeper package to the tank server:

        ./deploy.py install zookeeper dptst

* Bootstrap the cluster, this is only needed once when the cluster is setup
for the first time:

        ./deploy.py bootstrap zookeeper dptst

Here's some handy way to manage the cluster:

* Show the status of the zookeeper service:

        ./deploy.py show zookeeper dptst
* Start/Stop/Restart the zookeeper cluster:

        ./deploy.py stop zookeeper dptst
        ./deploy.py start zookeeper dptst
        ./deploy.py restart zookeeper dptst

* Cleanup the zookeeper cluster:

        ./deploy.py cleanup zookeeper dptst

* Rolling update the zookeeper cluster:

        ./deploy.py rolling_update zookeeper dptst

### Config Hdfs
As we have already introduced the cluster naming conventions, we will setup a
testing hdfs cluster named dptst-example, so the corresponding config file
will be hdfs-dptst-example.cfg.

Users can edit hdfs-dptst-example.cfg under the config/conf/hdfs directory
to config the cluster. The hdfs-dptst-example.cfg is well commented and self
explained, so we will net explain it here.

### Setup a Hdfs Cluster
To setup and manage a hdfs cluster, all the steps are the same as zookeeper's.
The only difference is the cluster name, dptst-example which implies the
corresponding zookeeper cluster dptst:

    ./deploy.py install hdfs dptst-example
    ./deploy.py bootstrap hdfs dptst-example
    ./deploy.py show hdfs dptst-example
    ./deploy.py stop hdfs dptst-example
    ./deploy.py start hdfs dptst-example
    ./deploy.py restart hdfs dptst-example
    ./deploy.py rolling_update hdfs dptst-example --job=datanode
    ./deploy.py cleanup hdfs dptst-example

### Shell
The client tool also supports a very handy command named shell. Users can use
this command to manage the files on hdfs, tables on hbase, jobs on yarn, and
etc. Here are some examples of hdfs shell comand:

    ./deploy.py shell hdfs dptst-example dfs -ls /
    ./deploy.py shell hdfs dptst-example dfs -mkdir /test
    ./deploy.py shell hdfs dptst-example dfs -rm -R /test

Users can run './deploy.py --help' to see the detailed help messages.
