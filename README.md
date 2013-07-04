# What is Minos

Minos is a distributed deployment and monitoring system.  It was initially developed and used at [Xiaomi](http://www.xiaomi.com) to deploy and manage the Hadoop, HBase and ZooKeeper clusters used in the company.  Minos can be easily extended to support other systems, among which HDFS, YARN and Impala have been supported in the current release.

# Components

The Minos system contains the following four components:

1. Client
2. Owl
3. Supervisor
4. Tank

## Client

This is the command line client tool used to deploy and manage processes of various systems.  You can refer to file `client/README.md` to learn how to use it.

## Owl

This is the dashboard system to display the status of all processes, where users can take a overview of the whole clusters managed by Minos.  You can refer to `owl/README.md` to learn how to use it.

## Supervisor

This is the process management and monitoring system.  You can refer to `supervisor/README.md` to learn how to use it.

## Tank

This is the package management system.  You can refer to `tank/README.md` to learn how to use it.
