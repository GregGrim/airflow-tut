import csv
import os
import smtplib
import zipfile
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List

import pendulum
import sqlalchemy as sa

from airflow.decorators import dag, task
from airflow.models import Variable

from app.repositories.log_repository import LogRepository


@dag(
    dag_id="send_logs_dag",
    start_date=pendulum.datetime(2016, 1, 1),
    schedule_interval=None,
    default_args={"retries": 1},
)
def send_logs_dag():
    @task(task_id="collect_logs_from_db")
    def collect_logs_from_db():
        db_url = Variable.get('db_url')
        log_repo = LogRepository(engine=sa.create_engine(db_url))
        logs = [log.model_dump() for log in log_repo.get_all_logs()]
        return logs

    @task(task_id="pack_logs_in_csv_zip")
    def pack_logs_in_csv_zip(logs: List[dict]):
        csv_filename = "logs.csv"
        zip_filename = "logs.zip"
        with open(csv_filename, mode='w', newline='') as file:
            fieldnames = logs[0].keys()
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            for row in logs:
                writer.writerow(row)

        with zipfile.ZipFile(zip_filename, mode='w', compression=zipfile.ZIP_DEFLATED) as archive:
            archive.write(csv_filename, arcname=csv_filename)

        os.remove(csv_filename)
        return "zip file created"

    @task(task_id="send_email_with_zip_attachment")
    def send_email_with_zip_attachment(**kwargs):
        receiver_email = kwargs["dag_run"].conf.get("email")
        sender_email = Variable.get("email")
        email_password = Variable.get("email_password")
        body = "Find all user logs in zip file attached."
        attachment_path = "logs.zip"
        smtp_server = "smtp.gmail.com"
        port = 465

        msg = MIMEMultipart()
        msg["From"] = sender_email
        msg["To"] = receiver_email
        msg["Subject"] = "Logs"
        msg.attach(MIMEText(body, "plain"))

        with open(attachment_path, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", f'attachment; filename={attachment_path}')
            msg.attach(part)
        try:
            with smtplib.SMTP_SSL(smtp_server, port) as server:
                server.login(sender_email, email_password)
                server.sendmail(sender_email, receiver_email, msg.as_string())
                os.remove("logs.zip")
                return {"details": "Email sent successfully!"}
        except Exception as e:
            return {"error": f"Failed to send email. Error: {e}"}

    logs_dict = collect_logs_from_db()
    pack_logs_in_csv_zip(logs_dict) >> send_email_with_zip_attachment()



dag = send_logs_dag()
