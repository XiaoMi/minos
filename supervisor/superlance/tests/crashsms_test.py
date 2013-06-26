import unittest
import mock
import time
from StringIO import StringIO

from crashmailbatch_test import CrashMailBatchTests

class CrashSMSTests(CrashMailBatchTests):
    subject = 'None'
    unexpected_err_msg = '[bar:foo](58597) exited unexpectedly'

    def _get_target_class(self):
        from superlance.crashsms import CrashSMS
        return CrashSMS

if __name__ == '__main__':
    unittest.main()