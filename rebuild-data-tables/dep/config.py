import logging
from enum import Enum, unique

# GLOBAL CONSTANTS
ATHENA_QUERIES_OUTPUT_PATH = "s3://datainsightsplatform-prod-athena-queries/uc1/"
ATHENA_WORKGROUP_NAME = "uc1"
# DEBUG CONSTANTS
DEBUG = False  # if debug_flag is set, other constants can be set here for testing
ENVIRONMENT_LOCAL = False  # set true if running locally, false if running in AWS Lambda
QC_EMAIL_EXTERNAL_SENDING_DISABLED = True
NUMBER_OF_ROWS_IN_PROD = 2580401
NUMBER_OF_ROWS_IN_PROD_SNAPSHOT = 2580385
NUMBER_OF_ROWS_IN_STAGING = 150692057


@unique
# WARNING, THIS IS NOT SETUP FOR NEGATIVES. ASSUMES HIGHER VALUES = WORSE OUTCOME FROM QC CHECKS. ORDER ALSO MATTERS.
class qc_level(Enum):
    SUCCESS_FULL = 10
    SUCCESS_WITH_NOTICE = 15
    FAILURE_SOFT = 20
    FAILURE_HARD = 30


logger = logging.getLogger(__name__)
