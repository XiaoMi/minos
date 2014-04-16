import owl_config
import smtplib
from email.mime.text import MIMEText

class Mailer:
  def __init__(self, options):
    self.options = options
    self.from_email = owl_config.ALERT_FROM_EMAIL
    self.smtp_host = owl_config.SMTPHOST
    self.password = owl_config.ROBOT_EMAIL_PASSWORD

  def send_email(self, content, subject, to_email, type='plain'):
    send_email(content = content,
               subject = subject,
               from_email = self.from_email,
               to_email = to_email,
               smtp_host = self.smtp_host,
               password = self.password,
               type = type,
              )

def send_email(subject, content, from_email, to_email, smtp_host, password, type):
  if not to_email:
    return

  msg = MIMEText(content, type)
  msg['Subject'] = subject
  msg['From'] = from_email
  to_emails = [addr.strip() for addr in to_email.split(',')]
  msg['To'] = ','.join(to_emails)

  connected = False
  try:
    smtp = smtplib.SMTP(smtp_host)
    if password:
      smtp.login(from_email.split('@')[0], password)
      connected = True

    smtp.sendmail(msg['From'], to_emails, msg.as_string())
  except Exception as e:
    print 'Send email failed: %r' % e
    if connected:
      smtp.quit()
