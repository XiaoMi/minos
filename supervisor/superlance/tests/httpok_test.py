import sys
import time
import unittest
from StringIO import StringIO
from supervisor.process import ProcessStates
from superlance.tests.dummy import *

_NOW = time.time()

_FAIL = [ {
        'name':'FAILED',
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
        'name':'SPAWN_ERROR',
        'group':'foo',
        'pid':11,
        'state':ProcessStates.RUNNING,
        'statename':'RUNNING',
        'start':_NOW - 100,
        'stop':0,
        'spawnerr':'',
        'now':_NOW,
        'description':'foo description',
        },]

def make_connection(response, exc=None):
    class TestConnection:
        def __init__(self, hostport):
            self.hostport = hostport

        def request(self, method, path):
            if exc:
                raise ValueError('foo')
            self.method = method
            self.path = path

        def getresponse(self):
            return response

    return TestConnection

class HTTPOkTests(unittest.TestCase):
    def _getTargetClass(self):
        from superlance.httpok import HTTPOk
        return HTTPOk
    
    def _makeOne(self, *opts):
        return self._getTargetClass()(*opts)

    def _makeOnePopulated(self, programs, any, response=None, exc=None,
                          gcore=None, coredir=None, eager=True):
        if response is None:
            response = DummyResponse()
        rpc = DummyRPCServer()
        sendmail = 'cat - > /dev/null'
        email = 'chrism@plope.com'
        url = 'http://foo/bar'
        timeout = 10
        status = '200'
        inbody = None
        gcore = gcore
        coredir = coredir
        prog = self._makeOne(rpc, programs, any, url, timeout, status,
                             inbody, email, sendmail, coredir, gcore, eager)
        prog.stdin = StringIO()
        prog.stdout = StringIO()
        prog.stderr = StringIO()
        prog.connclass = make_connection(response, exc=exc)
        return prog

    def test_listProcesses_no_programs(self):
        programs = []
        any = None
        prog = self._makeOnePopulated(programs, any)
        specs = list(prog.listProcesses())
        self.assertEqual(len(specs), 0)

    def test_listProcesses_w_RUNNING_programs_default_state(self):
        programs = ['foo']
        any = None
        prog = self._makeOnePopulated(programs, any)
        specs = list(prog.listProcesses())
        self.assertEqual(len(specs), 1)
        self.assertEqual(specs[0],
                         DummySupervisorRPCNamespace.all_process_info[0])

    def test_listProcesses_w_nonRUNNING_programs_default_state(self):
        programs = ['bar']
        any = None
        prog = self._makeOnePopulated(programs, any)
        specs = list(prog.listProcesses())
        self.assertEqual(len(specs), 1)
        self.assertEqual(specs[0],
                         DummySupervisorRPCNamespace.all_process_info[1])

    def test_listProcesses_w_nonRUNNING_programs_RUNNING_state(self):
        programs = ['bar']
        any = None
        prog = self._makeOnePopulated(programs, any)
        specs = list(prog.listProcesses(ProcessStates.RUNNING))
        self.assertEqual(len(specs), 0, (prog.programs, specs))

    def test_runforever_eager_notatick(self):
        programs = {'foo':0, 'bar':0, 'baz_01':0 }
        groups = {}
        any = None
        prog = self._makeOnePopulated(programs, any)
        prog.stdin.write('eventname:NOTATICK len:0\n')
        prog.stdin.seek(0)
        prog.runforever(test=True)
        self.assertEqual(prog.stderr.getvalue(), '')

    def test_runforever_eager_error_on_request_some(self):
        programs = ['foo', 'bar', 'baz_01', 'notexisting']
        any = None
        prog = self._makeOnePopulated(programs, any, exc=True)
        prog.stdin.write('eventname:TICK len:0\n')
        prog.stdin.seek(0)
        prog.runforever(test=True)
        lines = prog.stderr.getvalue().split('\n')
        #self.assertEqual(len(lines), 7)
        self.assertEqual(lines[0],
                         ("Restarting selected processes ['foo', 'bar', "
                          "'baz_01', 'notexisting']")
                         )
        self.assertEqual(lines[1], 'foo is in RUNNING state, restarting')
        self.assertEqual(lines[2], 'foo restarted')
        self.assertEqual(lines[3], 'bar not in RUNNING state, NOT restarting')
        self.assertEqual(lines[4],
                         'baz:baz_01 not in RUNNING state, NOT restarting')
        self.assertEqual(lines[5],
          "Programs not restarted because they did not exist: ['notexisting']")
        mailed = prog.mailed.split('\n')
        self.assertEqual(len(mailed), 12)
        self.assertEqual(mailed[0], 'To: chrism@plope.com')
        self.assertEqual(mailed[1],
                    'Subject: httpok for http://foo/bar: bad status returned')

    def test_runforever_eager_error_on_request_any(self):
        programs = []
        any = True
        prog = self._makeOnePopulated(programs, any, exc=True)
        prog.stdin.write('eventname:TICK len:0\n')
        prog.stdin.seek(0)
        prog.runforever(test=True)
        lines = prog.stderr.getvalue().split('\n')
        #self.assertEqual(len(lines), 6)
        self.assertEqual(lines[0], 'Restarting all running processes')
        self.assertEqual(lines[1], 'foo is in RUNNING state, restarting')
        self.assertEqual(lines[2], 'foo restarted')
        self.assertEqual(lines[3], 'bar not in RUNNING state, NOT restarting')
        self.assertEqual(lines[4],
                         'baz:baz_01 not in RUNNING state, NOT restarting')
        mailed = prog.mailed.split('\n')
        self.assertEqual(len(mailed), 11)
        self.assertEqual(mailed[0], 'To: chrism@plope.com')
        self.assertEqual(mailed[1],
                    'Subject: httpok for http://foo/bar: bad status returned')

    def test_runforever_eager_error_on_process_stop(self):
        programs = ['FAILED']
        any = False
        prog = self._makeOnePopulated(programs, any, exc=True)
        prog.rpc.supervisor.all_process_info = _FAIL
        prog.stdin.write('eventname:TICK len:0\n')
        prog.stdin.seek(0)
        prog.runforever(test=True)
        lines = prog.stderr.getvalue().split('\n')
        #self.assertEqual(len(lines), 5)
        self.assertEqual(lines[0], "Restarting selected processes ['FAILED']")
        self.assertEqual(lines[1], 'foo:FAILED is in RUNNING state, restarting')
        self.assertEqual(lines[2],
                    "Failed to stop process foo:FAILED: <Fault 30: 'FAILED'>")
        self.assertEqual(lines[3], 'foo:FAILED restarted')
        mailed = prog.mailed.split('\n')
        self.assertEqual(len(mailed), 10)
        self.assertEqual(mailed[0], 'To: chrism@plope.com')
        self.assertEqual(mailed[1],
                    'Subject: httpok for http://foo/bar: bad status returned')

    def test_runforever_eager_error_on_process_start(self):
        programs = ['SPAWN_ERROR']
        any = False
        prog = self._makeOnePopulated(programs, any, exc=True)
        prog.rpc.supervisor.all_process_info = _FAIL
        prog.stdin.write('eventname:TICK len:0\n')
        prog.stdin.seek(0)
        prog.runforever(test=True)
        lines = prog.stderr.getvalue().split('\n')
        #self.assertEqual(len(lines), 4)
        self.assertEqual(lines[0],
                         "Restarting selected processes ['SPAWN_ERROR']")
        self.assertEqual(lines[1],
                         'foo:SPAWN_ERROR is in RUNNING state, restarting')
        self.assertEqual(lines[2],
           "Failed to start process foo:SPAWN_ERROR: <Fault 50: 'SPAWN_ERROR'>")
        mailed = prog.mailed.split('\n')
        self.assertEqual(len(mailed), 9)
        self.assertEqual(mailed[0], 'To: chrism@plope.com')
        self.assertEqual(mailed[1],
                    'Subject: httpok for http://foo/bar: bad status returned')

    def test_runforever_eager_gcore(self):
        programs = ['foo', 'bar', 'baz_01', 'notexisting']
        any = None
        prog = self._makeOnePopulated(programs, any, exc=True, gcore="true",
                                      coredir="/tmp")
        prog.stdin.write('eventname:TICK len:0\n')
        prog.stdin.seek(0)
        prog.runforever(test=True)
        lines = prog.stderr.getvalue().split('\n')
        self.assertEqual(lines[0],
                         ("Restarting selected processes ['foo', 'bar', "
                          "'baz_01', 'notexisting']")
                         )
        self.assertEqual(lines[1], 'gcore output for foo:')
        self.assertEqual(lines[2], '')
        self.assertEqual(lines[3], ' ')
        self.assertEqual(lines[4], 'foo is in RUNNING state, restarting')
        self.assertEqual(lines[5], 'foo restarted')
        self.assertEqual(lines[6], 'bar not in RUNNING state, NOT restarting')
        self.assertEqual(lines[7],
                         'baz:baz_01 not in RUNNING state, NOT restarting')
        self.assertEqual(lines[8],
          "Programs not restarted because they did not exist: ['notexisting']")
        mailed = prog.mailed.split('\n')
        self.assertEqual(len(mailed), 15)
        self.assertEqual(mailed[0], 'To: chrism@plope.com')
        self.assertEqual(mailed[1],
                    'Subject: httpok for http://foo/bar: bad status returned')

    def test_runforever_not_eager_none_running(self):
        programs = ['bar', 'baz_01']
        any = None
        prog = self._makeOnePopulated(programs, any, exc=True, gcore="true",
                                      coredir="/tmp", eager=False)
        prog.stdin.write('eventname:TICK len:0\n')
        prog.stdin.seek(0)
        prog.runforever(test=True)
        lines = filter(None, prog.stderr.getvalue().split('\n'))
        self.assertEqual(len(lines), 0, lines)
        self.failIf('mailed' in prog.__dict__)

    def test_runforever_not_eager_running(self):
        programs = ['foo', 'bar']
        any = None
        prog = self._makeOnePopulated(programs, any, exc=True, eager=False)
        prog.stdin.write('eventname:TICK len:0\n')
        prog.stdin.seek(0)
        prog.runforever(test=True)
        lines = filter(None, prog.stderr.getvalue().split('\n'))
        self.assertEqual(lines[0],
                         ("Restarting selected processes ['foo', 'bar']")
                         )
        self.assertEqual(lines[1], 'foo is in RUNNING state, restarting')
        self.assertEqual(lines[2], 'foo restarted')
        self.assertEqual(lines[3], 'bar not in RUNNING state, NOT restarting')
        mailed = prog.mailed.split('\n')
        self.assertEqual(len(mailed), 10)
        self.assertEqual(mailed[0], 'To: chrism@plope.com')
        self.assertEqual(mailed[1],
                    'Subject: httpok for http://foo/bar: bad status returned')

if __name__ == '__main__':
    unittest.main()