import logging
from sys import platform

import boto3
from botocore.exceptions import ClientError

from typing import Dict
from . import config, sendemail

logger = logging.getLogger(__name__)


class emailMessage:
    Test_Purpose: str
    Test_QC_Result: str
    Test_Summary_Message: str

    def __init__(self):
        self.Test_Purpose = ''
        self.Test_QC_Result = ''
        self.Test_Summary_Message = ''


emailTable: Dict[str, emailMessage] = {}


def current_os():
    if platform == "linux" or platform == "linux2":
        return "linux"
    elif platform == "darwin":
        return "osx"
    elif platform == "win32":
        return "windows"


def send_sns(event, context, message, qc_result):
    '''
    send_sns_combined Sends an SNS notifying subscribers
    of staging outcome

    :param event: AWS Lambda uses this to pass in event data.
    :type event: Python type - Dict / list / int / string / float / None
    :param context: AWS Lambda uses this to pass in runtime information.
    :type context: LambdaContext
    '''

    sns_client = boto3.client("sns")
    SNSTopicARN = "arn:aws:sns:ap-southeast-2:958395584336:proddatalake-rebuild-data-tables-notification"
    file_type = event['fileType']

    message = message if isinstance(message, str) and len(
        message) > 0 else "No message provided"

    if qc_result == config.qc_level.SUCCESS_FULL:
        subject = 'DIP - Successfully rebuilt data tables for {}'.format(
            file_type)
    elif qc_result == config.qc_level.SUCCESS_WITH_NOTICE:
        subject = 'DIP - Successfully (with notices) rebuilt data tables for {}'.format(
            file_type)
    elif qc_result == config.qc_level.FAILURE_SOFT:
        subject = 'DIP - Failed some qc checks and rolled backed data tables for {}'.format(
            file_type)
    elif qc_result == config.qc_level.FAILURE_HARD:
        subject = 'DIP - Failed rebuilding data tables for {}'.format(
            file_type)
    else:
        subject = 'Error occurred when processing files'

    try:  # debug_flag will disable spammed by emails when testing. Can be enabled of disabled as required.
        # if config.DEBUG:
        #     logger.debug('SNS_subject: ' + subject[:100])
        #     logger.debug('SNS_message: ' + message)
        # else:
        if SNSTopicARN is not None:
            sns_client.publish(TopicArn=SNSTopicARN,
                               Subject=subject[:100], Message=message)

    except ClientError as e:
        # Validate if is this:
        # An error occurred (NotFound) when calling the GetTopicAttributes operation: Topic does not exist
        raise Exception('Error trying to send sns message: {}'.format(e))


def create_email(event, context, qcResults, result):

    file_type = event['fileType']
    department = event['combinedMetadata']['creator']
    email_recipient = event['combinedMetadata']['custodianEmail']
    dataset = event['combinedMetadata']['sourceDataset']

    # create email_subject
    if result == config.qc_level.SUCCESS_FULL:
        email_subject = 'DIP - Successfully rebuilt data tables for {dataset} from {department}'.format(
            dataset=dataset, department=department)
    elif result == config.qc_level.SUCCESS_WITH_NOTICE:
        email_subject = 'DIP - Successfully (with notices) rebuilt data tables for {dataset} from {department}'.format(
            dataset=dataset, department=department)
    elif result == config.qc_level.FAILURE_SOFT:
        email_subject = 'DIP - Failed some qc checks and rolled backed data tables for {dataset} from {department}'.format(
            dataset=dataset, department=department)
    elif result == config.qc_level.FAILURE_HARD:
        email_subject = 'DIP - Failed rebuilding data tables for {dataset} from {department}'.format(
            dataset=dataset, department=department)
    else:
        email_subject = 'Error occurred when processing files'

    # create email_body title from dataset and department.

    if result.name == "SUCCESS_FULL":
        emailTitleResult = 'Success'
    elif result.name == "SUCCESS_WITH_NOTICE":
        emailTitleResult = 'Success With Notice'
    else:
        emailTitleResult = 'Failure'

    emailTitle = 'Data validation results for ' + \
        dataset + \
        ' data from ' + \
        department + \
        ' - ' + \
        emailTitleResult

    # store each tests purpose, result and summary in dictionary
    for s in qcResults:
        if s.LogLevel == 'UserEmail':
            if s.TestID not in emailTable:
                emailTable[s.TestID] = emailMessage()
            emailmsg = emailTable[s.TestID]

            if s.Type == 'Test_Purpose':
                emailmsg.Test_Purpose = s.Message
            if s.Type == 'Test_QC_Result':
                emailmsg.Test_QC_Result = s.qcResult.name
            if s.Type == 'Test_Summary_Message':
                emailmsg.Test_Summary_Message = s.Message

    emailTableResults = ''
    # concat summary of test results into single html string
    for n in emailTable:
        if n != 'UserEmail':
            if emailTable[n].Test_QC_Result == 'SUCCESS_FULL':
                Test_qcResult_icon = '&#x1F7E2;'
                Test_Summary_Message = 'Success'
            elif emailTable[n].Test_QC_Result == 'SUCCESS_WITH_NOTICE':
                Test_qcResult_icon = '&#x1F7E1;'
                Test_Summary_Message = emailTable[n].Test_Summary_Message
            else:
                Test_qcResult_icon = '&#x1F534;'
                Test_Summary_Message = emailTable[n].Test_Summary_Message

            emailTableResults = emailTableResults + \
                '<tr><td><p>' + \
                emailTable[n].Test_Purpose + \
                '</p></td><td align="center"><p>' + \
                Test_qcResult_icon + \
                '</p></td><td><p>' + \
                Test_Summary_Message + \
                '</p></td></tr>'

    email_body_template = ''

    dictEmailBody = {}
    dictEmailBody.update(
        {'emailTitle': emailTitle, 'emailSummaryTable': emailTableResults})

    with open('dep/UpdatedEmailTemplate.html') as f:
        for line in f:
            email_body_template = email_body_template + \
                line.format(**dictEmailBody)

    sendemail.send_email(email_recipient, email_subject, email_body_template)
