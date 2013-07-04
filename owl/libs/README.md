## File
Library for zookeeper c client
    libzookeeper_mt.so.2 
Library for zkpython
    zookeeper.so

##Build method(on CentOS 6)

1. sudo yum install cppunit-devel
2. cd zookeeper
3. mvn clean package
4. cd zookeeper/src/c
5. autoreconf -if
6. ./configure
7. make
8. sudo make install
9. cd zookeeper/src/contrib/zkpython
10. ant build
