import boto3
from botocore.exceptions import ClientError

from . import config


def send_email(email_recipient, email_subject, email_body):

    # Replace sender@example.com with your "From" address.
    # This address must be verified.
    SENDER = "Data Insights <datainsights@dpc.wa.gov.au>"
    AWS_REGION = "ap-southeast-2"

    # The character encoding for the email.
    CHARSET = "UTF-8"

    # Replace recipient@example.com with a "To" address. If your account
    # is still in the sandbox, this address must be verified.
    RECIPIENT = email_recipient

    # The subject line for the email.
    SUBJECT = email_subject

    # The email body for recipients with non-HTML email clients.
    BODY_TEXT = (
        "Please view email on a HTML compatible client, e.g., MS Outlook")

    # The email body for recipients with HTML email clients.
    BODY_HTML = email_body

    client = boto3.client('ses', region_name=AWS_REGION)

    if config.QC_EMAIL_EXTERNAL_SENDING_DISABLED is False or email_recipient == "datainsights@dpc.wa.gov.au":
        # Try to send the email.
        try:
            response = client.send_email(
                Destination={
                    'ToAddresses': [
                        RECIPIENT,
                    ],
                },
                Message={
                    'Body': {
                        'Html': {
                            'Charset': CHARSET,
                            'Data': BODY_HTML,
                        },
                        'Text': {
                            'Charset': CHARSET,
                            'Data': BODY_TEXT,
                        },
                    },
                    'Subject': {
                        'Charset': CHARSET,
                        'Data': SUBJECT,
                    },
                },
                Source=SENDER
            )
        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            print("Email sent! Message ID:"),
            print(response['MessageId'])
    else:
        print("Sending to external email accounts disabled in config.py")
