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

##############################################################################
# crashsms
# author: Juan Batiz-Benet (http://github.com/jbenet)
# based on crashmailbatch.py
##############################################################################


# A event listener meant to be subscribed to PROCESS_STATE_CHANGE
# events.  It will send mail when processes that are children of
# supervisord transition unexpectedly to the EXITED state.

# A supervisor config snippet that tells supervisor to use this script
# as a listener is below.
#
# [eventlistener:crashsms]
# command=python crashsms -t <mobile phone>@<mobile provider> -f me@bar.com -e TICK_5
# events=PROCESS_STATE,TICK_5

doc = """\
crashsms.py [--interval=<batch interval in minutes>]
        [--toEmail=<email address>]
        [--fromEmail=<email address>]
        [--subject=<email subject>]

Options:

-i,--interval  - batch cycle length (in minutes).  The default is 1 minute.
                 This means that all events in each cycle are batched together
                 and sent as a single email

-t,--toEmail   - the email address to send alerts to. Mobile providers
                 tend to allow sms messages to be sent to their phone numbers
                 via an email address (e.g.: 1234567890@txt.att.net)

-f,--fromEmail - the email address to send alerts from

-s,--subject   - the email subject line

-e, --tickEvent - specify which TICK event to use (e.g. TICK_5, TICK_60, TICK_3600)

A sample invocation:

crashsms.py -t <mobile phone>@<mobile provider> -f me@bar.com -e TICK_5

"""

from supervisor import childutils
from superlance.process_state_email_monitor import ProcessStateEmailMonitor

class CrashSMS(ProcessStateEmailMonitor):
  process_state_events = ['PROCESS_STATE_EXITED']

  def __init__(self, **kwargs):
    ProcessStateEmailMonitor.__init__(self, **kwargs)
    self.now = kwargs.get('now', None)

  def get_process_state_change_msg(self, headers, payload):
    pheaders, pdata = childutils.eventdata(payload+'\n')

    if int(pheaders['expected']):
        return None

    txt = '[%(groupname)s:%(processname)s](%(pid)s) exited unexpectedly' \
      % pheaders
    return '%s %s' % (txt, childutils.get_asctime(self.now))

def main():
  crash = CrashSMS.create_from_cmd_line()
  crash.run()

if __name__ == '__main__':
  main()