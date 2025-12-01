"""
email_alerts.py
Sends monitoring alerts via email.
Uses: SMTP (Gmail), SendGrid, or enterprise SMTP gateway.
"""

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


def send_email_alert(subject, messages, env):
    sender = env.get("email_sender")
    receiver = env.get("email_receiver")
    password = env.get("email_password")   # App password or SMTP token

    body = "\n".join(messages)
    msg = MIMEMultipart()
    msg["From"] = sender
    msg["To"] = receiver
    msg["Subject"] = subject

    msg.attach(MIMEText(body, "plain"))

    try:
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(sender, password)
        server.sendmail(sender, receiver, msg.as_string())
        server.quit()

        print("Email alert sent.")
    except Exception as e:
        print(f"Failed to send alert email: {e}")
