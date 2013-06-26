import unittest
import mock
import time
from StringIO import StringIO

class ProcessStateEmailMonitorTestException(Exception):
    pass

class ProcessStateEmailMonitorTests(unittest.TestCase):
    from_email = 'testFrom@blah.com'
    to_emails = ('testTo@blah.com', 'testTo2@blah.com')
    to_str = 'testTo@blah.com, testTo2@blah.com'
    subject = 'Test Alert'
    
    def _get_target_class(self):
        from superlance.process_state_email_monitor \
        import ProcessStateEmailMonitor
        return ProcessStateEmailMonitor
    
    def _make_one(self, **kwargs):
        kwargs['stdin'] = StringIO()
        kwargs['stdout'] = StringIO()
        kwargs['stderr'] = StringIO()
        kwargs['from_email'] = kwargs.get('from_email', self.from_email)
        kwargs['to_emails'] = kwargs.get('to_emails', self.to_emails)
        kwargs['subject'] = kwargs.get('subject', self.subject)
        
        obj = self._get_target_class()(**kwargs)
        return obj
            
    def _make_one_mock_send_email(self, **kwargs):
        obj = self._make_one(**kwargs)
        obj.send_email = mock.Mock()
        return obj

    def _make_one_mock_send_smtp(self, **kwargs):
        obj = self._make_one(**kwargs)
        obj.send_smtp = mock.Mock()
        return obj
        
    def test_validate_cmd_line_options_single_to_email_ok(self):
        klass = self._get_target_class()
        
        options = mock.Mock()
        options.from_email = 'blah'
        options.to_emails = 'frog'
        
        validated = klass.validate_cmd_line_options(options)
        self.assertEquals(['frog'], validated.to_emails)

    def test_validate_cmd_line_options_multi_to_emails_ok(self):
        klass = self._get_target_class()
        
        options = mock.Mock()
        options.from_email = 'blah'
        options.to_emails = 'frog, log,dog'
        
        validated = klass.validate_cmd_line_options(options)
        self.assertEquals(['frog', 'log', 'dog'], validated.to_emails)
    
    def test_send_email_ok(self):
        email = {
            'body': 'msg1\nmsg2',
            'to': self.to_emails,
            'from': 'testFrom@blah.com',
            'subject': 'Test Alert',
        }
        monitor = self._make_one_mock_send_smtp()
        monitor.send_email(email)
        
        #Test that email was sent
        self.assertEquals(1, monitor.send_smtp.call_count)
        smtpCallArgs = monitor.send_smtp.call_args[0]
        mimeMsg = smtpCallArgs[0]
        self.assertEquals(self.to_str, mimeMsg['To'])
        self.assertEquals(email['from'], mimeMsg['From'])
        self.assertEquals(email['subject'], mimeMsg['Subject'])
        self.assertEquals(email['body'], mimeMsg.get_payload())

    def _raiseSTMPException(self, mime, to_emails):
        raise ProcessStateEmailMonitorTestException('test')
        
    def test_send_email_exception(self):
        email = {
            'body': 'msg1\nmsg2',
            'to': self.to_emails,
            'from': 'testFrom@blah.com',
            'subject': 'Test Alert',
        }
        monitor = self._make_one_mock_send_smtp()
        monitor.send_smtp.side_effect = self._raiseSTMPException
        monitor.send_email(email)

        #Test that error was logged to stderr
        self.assertEquals("Error sending email: test\n", monitor.stderr.getvalue())
    
    def test_send_batch_notification(self):
        test_msgs = ['msg1', 'msg2']
        monitor = self._make_one_mock_send_email()
        monitor.batchmsgs = test_msgs
        monitor.send_batch_notification()
        
        #Test that email was sent
        expected = {
            'body': 'msg1\nmsg2',
            'to': self.to_emails,
            'from': 'testFrom@blah.com',
            'subject': 'Test Alert',
        }
        self.assertEquals(1, monitor.send_email.call_count)
        monitor.send_email.assert_called_with(expected)
        
        #Test that email was logged
        self.assertEquals("""Sending notification email:
To: %s
From: testFrom@blah.com
Subject: Test Alert
Body:
msg1
msg2
""" % (self.to_str), monitor.stderr.getvalue())
        
    def test_log_email_with_body_digest(self):
        bodyLen = 80
        monitor = self._make_one_mock_send_email()
        email = {
            'to': ['you@fubar.com'],
            'from': 'me@fubar.com',
            'subject': 'yo yo',
            'body': 'a' * bodyLen,
        }
        monitor.log_email(email)
        self.assertEquals("""Sending notification email:
To: you@fubar.com
From: me@fubar.com
Subject: yo yo
Body:
aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa...
""", monitor.stderr.getvalue())
        self.assertEquals('a' * bodyLen, email['body'])

    def test_log_email_without_body_digest(self):
        monitor = self._make_one_mock_send_email()
        email = {
            'to': ['you@fubar.com'],
            'from': 'me@fubar.com',
            'subject': 'yo yo',
            'body': 'a' * 20,
        }
        monitor.log_email(email)
        self.assertEquals("""Sending notification email:
To: you@fubar.com
From: me@fubar.com
Subject: yo yo
Body:
aaaaaaaaaaaaaaaaaaaa
""", monitor.stderr.getvalue())

if __name__ == '__main__':
    unittest.main()