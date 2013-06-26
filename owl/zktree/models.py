from datetime import datetime
import threading
import zookeeper

PERM_READ = 1
PERM_WRITE = 2
PERM_CREATE = 4
PERM_DELETE = 8
PERM_ADMIN = 16
PERM_ALL = PERM_READ | PERM_WRITE | PERM_CREATE | PERM_DELETE | PERM_ADMIN

zookeeper.set_log_stream(open("cli_log.txt","w"))

TIMEOUT = 10.0

class ZKClient(object):
    def __init__(self, servers, timeout):
        self.connected = False
        self.conn_cv = threading.Condition( )
        self.handle = -1

        self.conn_cv.acquire()
        self.handle = zookeeper.init(servers, self.connection_watcher, 30000)
        self.conn_cv.wait(timeout)
        self.conn_cv.release()

        if not self.connected:
            raise Exception("Unable to connect to %s" % (servers))

    def connection_watcher(self, h, type, state, path):
        self.handle = h
        self.conn_cv.acquire()
        self.connected = True
        self.conn_cv.notifyAll()
        self.conn_cv.release()

    def close(self):
        zookeeper.close(self.handle)

    def get(self, path, watcher=None):
        return zookeeper.get(self.handle, path, watcher)

    def get_children(self, path, watcher=None):
        return zookeeper.get_children(self.handle, path, watcher)

    def get_acls(self, path):
        return zookeeper.get_acl(self.handle, path)

class ZNode(object):
    def __init__(self, addrs, path="/"):
        self.path = path
        zk = ZKClient(addrs, TIMEOUT)
        try:
            self.data, self.stat = zk.get(path)
            self.stat['ctime'] = datetime.fromtimestamp(self.stat['ctime']/1000)
            self.stat['mtime'] = datetime.fromtimestamp(self.stat['mtime']/1000)
            self.children = zk.get_children(path) or []
            self.acls = zk.get_acls(path)[1] or []
            for acl in self.acls:
                perms = acl['perms']
                perms_list = []
                if perms & PERM_READ:
                    perms_list.append("PERM_READ")
                if perms & PERM_WRITE:
                    perms_list.append("PERM_WRITE")
                if perms & PERM_CREATE:
                    perms_list.append("PERM_CREATE")
                if perms & PERM_DELETE:
                    perms_list.append("PERM_DELETE")
                if perms & PERM_ADMIN:
                    perms_list.append("PERM_ADMIN")
                if perms & PERM_ALL == PERM_ALL:
                    perms_list = ["PERM_ALL"]
                acl['perm_list'] = perms_list
        finally:
            zk.close()
