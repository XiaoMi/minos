# Supervisor - The Process Control System

[Supervisor](http://supervisord.org/) is an open source project, a client/server system that allows its users to monitor and control a number of processes on a UNIX-like operating system.

Based on the version of supervisor-3.0b1, we extended Supervisor to support Minos.  We implemented an RPC interface under the `deployment` directory, so that our deploy client can invoke the services supplied by supervisord.

## Prerequisites

    1. python 2.6 or later from <http://www.python.org/getit/>
    2. setuptools (latest) from <http://pypi.python.org/pypi/setuptools>
    3. meld3 (latest) from <http://www.plope.com/software/meld3/>
    4. elementtree (latest) from <http://effbot.org/downloads#elementtree>
    5. pexpect (latest) from <http://www.noah.org/wiki/pexpect>

## Deploying Supervisord

To deploy supervisord, first we should check out the code to our local working directory:

    git clone https://githum.com/xiaomi/minos.git

After checking out the code, we need to update the configuration file to configure information of the destination machines.  Here is a simple example:

    [group_1]
    ; The remote user
    user=work

    ; The remote user's password
    password=work

    ; The remote directory to install supervisor
    root_dir=/home/work/data

    ; The remote data directories available for applications
    data_dirs=/home/work/data

    ; The remote host list
    host.0=192.168.1.11
    host.1=192.168.1.12
    host.2=192.168.1.13

    [group_2]
    ; The remote user
    user=work

    ; The remote user's password
    password=work

    ; The remote directory to install supervisor
    root_dir=/home/work/data1

    ; The remote data directories available for applications
    data_dirs=/home/work/data1,/home/work/data2,/home/work/data3

    ; The remote host list
    host.0=192.168.1.14
    host.1=192.168.1.15
    host.2=192.168.1.16

Note that, in the above config example, we use different groups of machines to support heterogeneous hardware.  The above configuration file is located in `minos/config` directory.

After changing the configuration file, we can run the `deploy_supervisor.py` tool to install supervisor onto every remote machine:

    cd minos/supervisor
    ./deploy_supervisor.py
    cd -

After finishing the above deploy process, supervisord is started on all remote machines.  You can access the web interface of each supervisord.  For example, suppose the default port is 9001, you can access the following URL to view the processes managed by supervisord on 192.168.1.11:

    http://192.168.1.11:9001/

---

# Superlance

[Superlance](https://pypi.python.org/pypi/superlance) is a package of plug-in utilities for monitoring and controlling processes that run under supervisor.

We integrate `superlance-0.7` to our supervisor system, and use the crashmail tool to monitor all processes.  When a process exits unexpectedly, crashmail will send an alert email to a mailing list that is configurable.

We configure crashmail as an auto-started process.  It will start working automatically when the supervisor is started.  Following is a config example, taken from `minos/config/supervisord.conf`, that shows how to configure crashmail:

    [eventlistener:crashmailbatch-monitor]
    command=python superlance/crashmailbatch.py \
            --toEmail="alert@example.com" \
            --fromEmail="robot@example.com" \
            --password="123456" \
            --smtpHost="mail.example.com" \
            --tickEvent=TICK_5 \
            --interval=0.5
    events=PROCESS_STATE,TICK_5
    buffer_size=100
    stdout_logfile=crashmailbatch.stdout
    stderr_logfile=crashmailbatch.stderr
    autostart=true
