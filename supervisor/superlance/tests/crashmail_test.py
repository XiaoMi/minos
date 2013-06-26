import sys
import unittest
from StringIO import StringIO

class CrashMailTests(unittest.TestCase):
    def _getTargetClass(self):
        from superlance.crashmail import CrashMail
        return CrashMail
    
    def _makeOne(self, *opts):
        return self._getTargetClass()(*opts)

    def setUp(self):
        import tempfile
        self.tempdir = tempfile.mkdtemp()

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tempdir)

    def _makeOnePopulated(self, programs, any, response=None):

        import os
        sendmail = 'cat - > %s' % os.path.join(self.tempdir, 'email.log')
        email = 'chrism@plope.com'
        header = '[foo]'
        prog = self._makeOne(programs, any, email, sendmail, header)
        prog.stdin = StringIO()
        prog.stdout = StringIO()
        prog.stderr = StringIO()
        return prog

    def test_runforever_not_process_state_exited(self):
        programs = {'foo':0, 'bar':0, 'baz_01':0 }
        groups = {}
        any = None
        prog = self._makeOnePopulated(programs, any)
        prog.stdin.write('eventname:PROCESS_STATE len:0\n')
        prog.stdin.seek(0)
        prog.runforever(test=True)
        self.assertEqual(prog.stderr.getvalue(), 'non-exited event\n')

    def test_runforever_expected_exit(self):
        programs = ['foo']
        any = None
        prog = self._makeOnePopulated(programs, any)
        payload=('expected:1 processname:foo groupname:bar '
                 'from_state:RUNNING pid:1')
        prog.stdin.write(
            'eventname:PROCESS_STATE_EXITED len:%s\n' % len(payload))
        prog.stdin.write(payload)
        prog.stdin.seek(0)
        prog.runforever(test=True)
        self.assertEqual(prog.stderr.getvalue(), 'expected exit\n')

    def test_runforever_unexpected_exit(self):
        programs = ['foo']
        any = None
        prog = self._makeOnePopulated(programs, any)
        payload=('expected:0 processname:foo groupname:bar '
                 'from_state:RUNNING pid:1')
        prog.stdin.write(
            'eventname:PROCESS_STATE_EXITED len:%s\n' % len(payload))
        prog.stdin.write(payload)
        prog.stdin.seek(0)
        prog.runforever(test=True)
        output = prog.stderr.getvalue()
        lines = output.split('\n')
        self.assertEqual(lines[0], 'unexpected exit, mailing')
        self.assertEqual(lines[1], 'Mailed:')
        self.assertEqual(lines[2], '')
        self.assertEqual(lines[3], 'To: chrism@plope.com')
        self.failUnless('Subject: [foo]: foo crashed at' in lines[4])
        self.assertEqual(lines[5], '')
        self.failUnless(
            'Process foo in group bar exited unexpectedly' in lines[6])
        import os
        mail = open(os.path.join(self.tempdir, 'email.log'), 'r').read()
        self.failUnless(
            'Process foo in group bar exited unexpectedly' in mail)

if __name__ == '__main__':
    unittest.main()