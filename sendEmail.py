import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.utils import formataddr
from datetime import datetime

# Function to send email
def send_email(from_date, to_date, report_date, path, schedule):
    # Sender email address
    mail_from = f"{os.environ.get('USERNAME')}@Cotiviti.com".title()

    # Email body message
    message_body = f"""Hello,

            Please review the RCA Retail {schedule} Report Between {from_date} and {to_date}.

Thank You,
Pikesh Maharjan
"""

    # SMTP server configuration
    smtp_server = 'alerts.smtp.ccaintranet.net'

    def send_mail(mail_to, cc, bcc, subject, body, attachment_path=None):
        msg = MIMEMultipart()
        msg['From'] = mail_from
        msg['To'] = mail_to
        msg['Cc'] = cc
        msg['Bcc'] = bcc
        msg['Subject'] = subject

        # Attach the email body
        msg.attach(MIMEText(body, 'plain'))

        # Attach the file if it exists
        if attachment_path and os.path.exists(attachment_path):
            with open(attachment_path, 'rb') as attachment_file:
                attachment = MIMEApplication(attachment_file.read(), _subtype="xlsx")
                attachment.add_header('Content-Disposition', 'attachment', filename=os.path.basename(attachment_path))
                msg.attach(attachment)

        # Sending the email
        with smtplib.SMTP(smtp_server) as server:
            server.send_message(msg)

    # Check if the file exists and send email accordingly
    if os.path.exists(path):
        send_mail(
            mail_to="Neeraj.Manandhar@cotiviti.com",
            cc="Prakash.Manandhar@infinite.com",
            bcc="Pikesh.Maharjan@cotiviti.com",
            subject=f"{schedule} Report for Retail RCA for {report_date}",
            body=message_body,
            attachment_path=path
        )
    else:
        send_mail(
            mail_to="Pikesh.Maharjan@cotiviti.com",
            cc="",
            bcc="",
            subject=f"Weekly Report for Retail RCA for {report_date}",
            body="Report hasn't been created. Please verify."
        )

    print("Email sent successfully!")
