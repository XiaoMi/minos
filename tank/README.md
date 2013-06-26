# Tank - The Package Management Server
Tank is a simple package management django app server for our deployment tool.

## Prerequisites
Install the prerequisite packages:

    1. python 2.6 or later from <http://www.python.org/getit/>
    2. django 1.4 or later from <https://www.djangoproject.com/download/>

## Install Tank
To install the server, just check out the code on your production machine:

    git clone https://github.com/xiaomi/minos/tank.git

## Run Tank
To run the server, execute the following commands:

    cd $INSTALL_PATH
    ./start_tank.sh

## Directories
Here is a simple introduction of the directories of tank:

    data/: the data directory used to store the packages
    package_server/: the package server django app directory
    sqlite/: the sqlite database directory
    static/: the static resources directory
    tank/: the main django directory
    templates/: the web page template directory
