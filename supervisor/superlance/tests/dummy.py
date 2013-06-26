class DummyRPCServer:
    def __init__(self):
        self.supervisor = DummySupervisorRPCNamespace()
        self.system = DummySystemRPCNamespace()

class DummyResponse:
    status = 200
    reason = 'OK'
    body = 'OK'
    def read(self):
        return self.body 
        
class DummySystemRPCNamespace:
    pass


import time
from supervisor.process import ProcessStates

_NOW = time.time()
    
class DummySupervisorRPCNamespace:
    _restartable = True
    _restarted = False
    _shutdown = False
    _readlog_error = False


    all_process_info = [
        {
        'name':'foo',
        'group':'foo',
        'pid':11,
        'state':ProcessStates.RUNNING,
        'statename':'RUNNING',
        'start':_NOW - 100,
        'stop':0,
        'spawnerr':'',
        'now':_NOW,
        'description':'foo description',
        },
        {
        'name':'bar',
        'group':'bar',
        'pid':12,
        'state':ProcessStates.FATAL,
        'statename':'FATAL',
        'start':_NOW - 100,
        'stop':_NOW - 50,
        'spawnerr':'screwed',
        'now':_NOW,
        'description':'bar description',
        },
        {
        'name':'baz_01',
        'group':'baz',
        'pid':12,
        'state':ProcessStates.STOPPED,
        'statename':'STOPPED',
        'start':_NOW - 100,
        'stop':_NOW - 25,
        'spawnerr':'',
        'now':_NOW,
        'description':'baz description',
        },
        ]

    def getAllProcessInfo(self):
        return self.all_process_info

    def startProcess(self, name):
        from supervisor import xmlrpc
        from xmlrpclib import Fault
        if name.endswith('SPAWN_ERROR'):
            raise Fault(xmlrpc.Faults.SPAWN_ERROR, 'SPAWN_ERROR')
        return True

    def stopProcess(self, name):
        from supervisor import xmlrpc
        from xmlrpclib import Fault
        if name == 'BAD_NAME:BAD_NAME':
            raise Fault(xmlrpc.Faults.BAD_NAME, 'BAD_NAME:BAD_NAME') 
        if name.endswith('FAILED'):
            raise Fault(xmlrpc.Faults.FAILED, 'FAILED')
        return True

