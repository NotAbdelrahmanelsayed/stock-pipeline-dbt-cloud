import requests
from utils.constants import SLACK_WEBHOOK_URL


def send_slack_message(message, webhook_url):
    payload = {"text": message}
    requests.post(webhook_url, json=payload)


def notify_faliure(context):
    report = context["task_instance"].xcom_pull(key="validation_report")
    send_slack_message(f"Data Validation failed\n{report}", SLACK_WEBHOOK_URL)
