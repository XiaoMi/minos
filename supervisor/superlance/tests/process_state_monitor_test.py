import unittest
import mock
import time
from StringIO import StringIO
from superlance.process_state_monitor import ProcessStateMonitor

class TestProcessStateMonitor(ProcessStateMonitor):
    
    process_state_events = ['PROCESS_STATE_EXITED']
            
    def get_process_state_change_msg(self, headers, payload):
        return repr(payload)

class ProcessStateMonitorTests(unittest.TestCase):
    
    def _get_target_class(self):
        return TestProcessStateMonitor
        
    def _make_one_mocked(self, **kwargs):
        kwargs['stdin'] = StringIO()
        kwargs['stdout'] = StringIO()
        kwargs['stderr'] = StringIO()
        
        obj = self._get_target_class()(**kwargs)
        obj.send_batch_notification = mock.Mock()
        return obj

    def get_process_exited_event(self, pname, gname, expected,
                                eventname='PROCESS_STATE_EXITED'):
        headers = {
            'ver': '3.0', 'poolserial': '7', 'len': '71',
            'server': 'supervisor', 'eventname': eventname,
            'serial': '7', 'pool': 'checkmailbatch',
        }
        payload = 'processname:%s groupname:%s from_state:RUNNING expected:%d \
pid:58597' % (pname, gname, expected)
        return (headers, payload)
        
    def get_tick60_event(self):
        headers = {
            'ver': '3.0', 'poolserial': '5', 'len': '15',
            'server': 'supervisor', 'eventname': 'TICK_60',
            'serial': '5', 'pool': 'checkmailbatch',
        }
        payload = 'when:1279665240'
        return (headers, payload)

    def test__get_tick_secs(self):
        monitor = self._make_one_mocked()
        self.assertEquals(5, monitor._get_tick_secs('TICK_5'))
        self.assertEquals(60, monitor._get_tick_secs('TICK_60'))
        self.assertEquals(3600, monitor._get_tick_secs('TICK_3600'))
        self.assertRaises(ValueError, monitor._get_tick_secs, 'JUNK_60')

    def test__get_tick_mins(self):
        monitor = self._make_one_mocked()
        self.assertEquals(5.0/60.0, monitor._get_tick_mins('TICK_5'))
        
    def test_handle_event_exit(self):
        monitor = self._make_one_mocked()
        hdrs, payload = self.get_process_exited_event('foo', 'bar', 0)
        monitor.handle_event(hdrs, payload)
        unexpected_err_msg = repr(payload)
        self.assertEquals([unexpected_err_msg], monitor.get_batch_msgs())
        self.assertEquals('%s\n' % unexpected_err_msg, monitor.stderr.getvalue())

    def test_handle_event_non_exit(self):
        monitor = self._make_one_mocked()
        hdrs, payload = self.get_process_exited_event('foo', 'bar', 0,
                                            eventname='PROCESS_STATE_FATAL')
        monitor.handle_event(hdrs, payload)
        self.assertEquals([], monitor.get_batch_msgs())
        self.assertEquals('', monitor.stderr.getvalue())

    def test_handle_event_tick_interval_expired(self):
        monitor = self._make_one_mocked()
        #Put msgs in batch
        hdrs, payload = self.get_process_exited_event('foo', 'bar', 0)
        monitor.handle_event(hdrs, payload)
        hdrs, payload = self.get_process_exited_event('bark', 'dog', 0)
        monitor.handle_event(hdrs, payload)
        self.assertEquals(2, len(monitor.get_batch_msgs()))
        #Time expired
        hdrs, payload = self.get_tick60_event()
        monitor.handle_event(hdrs, payload)
        
        #Test that batch messages are now gone
        self.assertEquals([], monitor.get_batch_msgs())
        #Test that email was sent
        self.assertEquals(1, monitor.send_batch_notification.call_count)

    def test_handle_event_tick_interval_not_expired(self):
        monitor = self._make_one_mocked(interval=3)
        hdrs, payload = self.get_tick60_event()
        monitor.handle_event(hdrs, payload)
        self.assertEquals(1.0, monitor.get_batch_minutes())
        monitor.handle_event(hdrs, payload)
        self.assertEquals(2.0, monitor.get_batch_minutes())

if __name__ == '__main__':
    unittest.main()