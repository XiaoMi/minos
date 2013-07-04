# Xiaomi Minos
Xiaomi Minos is a distributed deployment and monitoring system used in
[XIAOMI.COM](http://www.xiaomi.com).  It's initially used to deploy and
manage our hadoop/hbase/zookeeper clusters, and it is very convenient to
extend to support deploying other services.  Currently, it supports
deploying zookeeper/hdfs/yarn/hbase/impala services.

# Components
The Minos system contains the following four components:

## Client
This is the command line client tool for users to deploy and manage processes
of various services. Users can refer to client/README.md to learn how to use
it.

## Owl
This is the process status dashboard system, from which users can take a
overview of the whole clusters managed by Minos. Users can refer to
owl/README.md to learn how to use it.

## Supervisor
This is the process management and monitor system. Users can refer to
supervisor/README.md to learn how to use it.

## Tank
This is the package management system. Users can refer to tank/README.md to
learn how to use it.

