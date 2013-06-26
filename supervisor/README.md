# Supervisor - The Process Control System
[Supervisor](http://supervisord.org/) is an opensource project, it is a
client/server system that allows its users to monitor and control a number
of processes on UNIX-like operating systems.

Based on the version of supervisor-3.0b1, we extend supervisor system to
support our deployment system. We implement a RPC interface under the
deployment directory, in this way our deploy client can invoke the services
supplied by supervisord.

## Prerequisites

    1. python 2.6 or later from <http://www.python.org/getit/>
    2. setuptools (latest) from <http://pypi.python.org/pypi/setuptools>
    3. meld3 (latest) from <http://www.plope.com/software/meld3/>
    4. elementtree (latest) from <http://effbot.org/downloads#elementtree>
    5. pexpect (latest) from <http://www.noah.org/wiki/pexpect>

## Deploy Supervisord
To deploy the supervisord, first we should check out the code to our local
working directory:

    git clone https://githum.com/xiaomi/minos.git

After checking out the code, we need to update the config file to config
the destination machines' information, here is a simple example:

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

Note: In the above config example, we use different groups of machines to
support heterogeneous hardware. And the above config file is located in
minos/config directory.

After finishing the config file, we can run the deploy_supervisor.py tool to
install supervisor to every remote machine:

    cd minos/supervisor
    ./deploy_supervisor.py
    cd -

After finishing the above deploy process, all the supervisords are started
on the remote machines. Users can access the web interface of each supervisord,
for example, suppose the default port is 9001, we can access the following url
to view the processes managed by the supervisord on 192.168.1.11:

    http://192.168.1.11:9001/

---
# Superlance
[Superlance](https://pypi.python.org/pypi/superlance) is a package of plugin
utilities for monitoring and controlling processes that run under supervisor.

We integrate superlance-0.7 to our supervisor system, and use the crashmail
tool to monitor our processes. When a proccess is exited unexpectedly, the
crashmail tool will send alert mail to our mail group, this is really very
handy.

We config the crashmail to supervisor as an auto-started process, it will
start working automatically when the supervisor is started. Following is a
config example taken from minos/config/supervisord.conf to show how to config
crashmail alertor:

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
