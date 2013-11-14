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
import os
import sys
import smtplib
import copy
# Using old reference for Python 2.4
from email.MIMEText import MIMEText
# from email.mime.text import MIMEText
from superlance.process_state_monitor import ProcessStateMonitor

doc = """\
Base class for common functionality when monitoring process state changes
and sending email notification
"""

class ProcessStateEmailMonitor(ProcessStateMonitor):
    COMMASPACE = ', '

    @classmethod
    def parse_cmd_line_options(cls):
        from optparse import OptionParser

        parser = OptionParser()
        parser.add_option("-i", "--interval", dest="interval", type="float", default=1.0,
                          help="batch interval in minutes (defaults to 1 minute)")
        parser.add_option("-t", "--toEmail", dest="to_emails",
                          help="destination email address(es) - comma separated")
        parser.add_option("-f", "--fromEmail", dest="from_email",
                          help="source email address")
        parser.add_option("-p", "--password", dest="password",
                          help="source email password")
        parser.add_option("-s", "--subject", dest="subject",
                          help="email subject")
        parser.add_option("-H", "--smtpHost", dest="smtp_host", default="localhost",
                          help="SMTP server hostname or address")
        parser.add_option("-e", "--tickEvent", dest="eventname", default="TICK_60",
                          help="TICK event name (defaults to TICK_60)")
        parser.add_option("-l", "--supervisordPort", dest="supervisord_port",
                          default=9001, help="the supervisord server port")
        
        (options, args) = parser.parse_args()
        return options
        
    @classmethod
    def validate_cmd_line_options(cls, options):
        if not options.to_emails:
            parser.print_help()
            sys.exit(1)
        if not options.from_email:
            parser.print_help()
            sys.exit(1)
        
        validated = copy.copy(options)
        validated.to_emails = [x.strip() for x in options.to_emails.split(",")]
        return validated
        
    @classmethod
    def get_cmd_line_options(cls):
        return cls.validate_cmd_line_options(cls.parse_cmd_line_options())

    @classmethod
    def create_from_cmd_line(cls):
        options = cls.get_cmd_line_options()

        if not 'SUPERVISOR_SERVER_URL' in os.environ:
            sys.stderr.write('Must run as a supervisor event listener\n')
            sys.exit(1)

        return cls(**options.__dict__)

    def __init__(self, **kwargs):
        ProcessStateMonitor.__init__(self, **kwargs)

        self.from_email = kwargs['from_email']
        self.to_emails = kwargs['to_emails']
        self.subject = kwargs.get('subject')
        self.smtp_host = kwargs.get('smtp_host', 'localhost')
        self.digest_len = 76
        self.password = kwargs.get('password')
        self.supervisord_port = kwargs.get('supervisord_port')
        self.customized_mail_list = []

    def send_batch_notification(self):
        email = self.get_batch_email()
        if email:
            self.send_email(email)
            self.log_email(email)

    def log_email(self, email):
        email_for_log = copy.copy(email)
        email_for_log['to'] = self.COMMASPACE.join(email['to'])
        if len(email_for_log['body']) > self.digest_len:
            email_for_log['body'] = '%s...' % email_for_log['body'][:self.digest_len]
        self.write_stderr("Sending notification email:\nTo: %(to)s\n\
From: %(from)s\nSubject: %(subject)s\nBody:\n%(body)s\n" % email_for_log)

    def get_batch_email(self):
        if len(self.batchmsgs):
            self.customized_mail_list.extend(self.to_emails)
            return {
                'to': self.customized_mail_list,
                'from': self.from_email,
                'subject': self.subject,
                'body': '\n'.join(self.get_batch_msgs()),
            }
        return None

    def send_email(self, email):
        msg = MIMEText(email['body'])
        if self.subject:
          msg['Subject'] = email['subject']
        msg['From'] = email['from']
        msg['To'] = self.COMMASPACE.join(email['to'])

        try:
            self.send_smtp(msg, email['to'])
        except Exception, e:
            self.write_stderr("Error sending email: %s\n" % e)

    def send_smtp(self, mime_msg, to_emails):
        s = smtplib.SMTP(self.smtp_host)
        if self.password:
            try:
                s.login(self.from_email.split('@')[0], self.password)
            except:
                s.quit()
                raise
        try:
            s.sendmail(mime_msg['From'], to_emails, mime_msg.as_string())
        except:
            s.quit()
            raise
        s.quit()

