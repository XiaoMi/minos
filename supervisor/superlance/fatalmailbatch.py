#!/usr/bin/env python -u
##############################################################################
#
# Copyright (c) 2007 Agendaless Consulting and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the BSD-like license at
# http://www.repoze.org/LICENSE.txt.  A copy of the license should accompany
# this distribution.  THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL
# EXPRESS OR IMPLIED WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO,
# THE IMPLIED WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND
# FITNESS FOR A PARTICULAR PURPOSE
#
##############################################################################

# A event listener meant to be subscribed to PROCESS_STATE_CHANGE
# events.  It will send mail when processes that are children of
# supervisord transition unexpectedly to the EXITED state.

# A supervisor config snippet that tells supervisor to use this script
# as a listener is below.
#
# [eventlistener:fatalmailbatch]
# command=python fatalmailbatch
# events=PROCESS_STATE,TICK_60

doc = """\
fatalmailbatch.py [--interval=<batch interval in minutes>]
        [--toEmail=<email address>]
        [--fromEmail=<email address>]
        [--subject=<email subject>]

Options:

--interval  - batch cycle length (in minutes).  The default is 1 minute.
                  This means that all events in each cycle are batched together
                  and sent as a single email
                  
--toEmail   - the email address to send alerts to

--fromEmail - the email address to send alerts from

--subject - the email subject line

A sample invocation:

fatalmailbatch.py --toEmail="you@bar.com" --fromEmail="me@bar.com"

"""

from supervisor import childutils
from superlance.process_state_email_monitor import ProcessStateEmailMonitor

class FatalMailBatch(ProcessStateEmailMonitor):
    
    process_state_events = ['PROCESS_STATE_FATAL']

    def __init__(self, **kwargs):
        kwargs['subject'] = kwargs.get('subject', 'Fatal start alert from supervisord')
        ProcessStateEmailMonitor.__init__(self, **kwargs)
        self.now = kwargs.get('now', None)
 
    def get_process_state_change_msg(self, headers, payload):
        pheaders, pdata = childutils.eventdata(payload+'\n')

        txt = 'Process %(groupname)s:%(processname)s failed to start too many \
times' % pheaders
        return '%s -- %s' % (childutils.get_asctime(self.now), txt)

def main():
    fatal = FatalMailBatch.create_from_cmd_line()
    fatal.run()

if __name__ == '__main__':
    main()