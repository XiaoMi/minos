# Owl
Owl is a monitor system for Hadoop cluster. It collects data from servers through JMX interface. And it organizes pages in cluster, job and task corresponding to the definition in cluster configuration. It also provides some utils like health alerter, HDFS quota updater and quota reportor.

# Requirements
mysql-server

python 2.7

python lib:

django 1.5.x <https://www.djangoproject.com/>

twisted <http://twistedmatrix.com/>

mysql-python <http://sourceforge.net/projects/mysql-python/>

dbutils <https://pypi.python.org/pypi/DBUtils/>

If you use pip(<http://www.pip-installer.org/>), you can install python libs like:

    pip install django

# Installation
Initialize Mysql

    mysql -uroot -ppassword
    >create database owl
    >use mysql;
    >GRANT ALL ON owl.* TO 'owl'@'localhost' identified by 'owl';
    >flush privileges;

Initialize Django
  
    python manage.py syncdb

# Configuration
Collector configuration

Modify collector.cfg in minos/config/owl to change configuration for monitor

    [collector]
    # service name(space seperated)
    service = hdfs hbase
    
    [hdfs] 
    # cluster name(space seperated)
    clusters=dptst-example
    # job name(space seperated)
    jobs=journalnode namenode datanode
    # url for collecotr, usually JMX url
    metric_url=/jmx?qry=Hadoop:*

Modify owl_config.py in minos/config/owl/owl_config.py to change configuration for owl

    # Opentsdb base url, for displaying metrics in owl
    TSDB_ADDR = 'http://127.0.0.1:9888'
    # Supervisor port, for jump to supervisor page in owl
    SUPERVISOR_PORT = '9001'

# Run
###Start collector
    ./collector.sh

###Start server
    ./runserver.sh 0.0.0.0:8000 

###Run quota updater [optional]
    ./quota_updater.sh

###Run quota reportor [optional]
    ./quota_repotor.sh

###Run health alert [optional]
    ./alert.sh 
