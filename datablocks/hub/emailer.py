import os
import smtplib
from email.message import EmailMessage

def send_email(*, to_whom, from_whom, subject, text, smtp_server='localhost'):
    msg = EmailMessage()
    msg.set_content(text)
    msg['Subject'] = subject
    msg['From'] = from_whom
    msg['To'] = to_whom

    s = smtplib.SMTP(smtp_server)
    s.send_message(msg)
    r = s .quit()
    return r