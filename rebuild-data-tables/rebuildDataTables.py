import hashlib
import io
import json
import locale
import logging
import os
import pickle
import sys
import time
import traceback
from datetime import date, datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import List

import boto3
from dateutil.relativedelta import relativedelta

import dep.config as config
import dep.functions as functions

# Not supported for Python 3.9 in Lambdas for some reason
# from typing import Literal
# TODO: Some checks need to be soft fails (alerts and warnings in QC). Need to be able to hard fail and soft fail with warning against 'failed' check.
# Soft fail: date ranges where most recent data not available but new data is included.
# Hard failures are things like unexpected values in columns.
# TODO: extra sql for all the datasets.
# TODO: extra checks on various sid datasets depending on their business and content.
# TODO: split out into files that make sense (main, utils, config). VB - started this, but need another two-three days to refactor out the sql and qc tests so the code is more readable (and other bits talked about through the code).

locale.setlocale(locale.LC_ALL, "en_US.UTF-8")
os.environ["TZ"] = "Australia/Perth"
if functions.current_os() == "linux":
    time.tzset()

# if running locally, send logging to local file
if config.ENVIRONMENT_LOCAL:
    logging.basicConfig(level=logging.DEBUG if config.DEBUG else logging.WARNING,
                        filename='rebuildDataTables_debug.log',
                        encoding='utf-8',
                        format='%(asctime)s %(name)s %(levelname)s:%(message)s')
else:
    logging.basicConfig(level=logging.DEBUG if config.DEBUG else logging.WARNING,
                        format='%(asctime)s %(name)s %(levelname)s:%(message)s')

logger = logging.getLogger(__name__)

logger.debug("Debug Log:")
logger.debug(functions.current_os())


class RebuildProdTableException(Exception):
    pass


class RebuildProdTableExceptionAlertOnly(Exception):
    pass

# ARCHITECTURAL AND ENVIRONMENTAL CHANGES
# changed uc1 workgroup settings to NOT override client settings
# role created for this lambda
# policy created for this lambda
# bucket created for this lambda
# lambda config = 128mb and 5mins timeout

# @TODO Attached 'AmazonAthenaFullAccess' policy to the lambda to make it work. Switch it out for a policy with the bare minimum scopes required.

# @TODO There's no VPC attached to the Lambda because the default VPC doesn't allow connections to Athena (so queries would just timeout)
# Work out how to address this in a proper and SECURE way
# Ref: https://docs.aws.amazon.com/athena/latest/ug/interface-vpc-endpoint.html

# @TODO What's the Lambda timeout? What should it be (rightsizing)?

# VB - use a constructor to handle all the prints the end up constructing the email (https://beginnersbook.com/2018/03/python-constructors-default-and-parameterized/) - do this


class qcMessage:
    Type: str  # Type: Debug, Test_Purpose, Test_QC_Result, Test_Summary_Message, Test_Messages
    Message: str  # [freetext]
    LogLevel: str  # LogLevel: 1 = Debug, 2 = UserEmail
    TestID: str  # MD5 calculated from settings
    qcResult: Enum  # outcome of qc_test

    def __init__(self, _Type: str, _Message: str, _LogLevel: str, _TestID: str = None, _qcResult: Enum = None):
        self.Type = _Type
        self.Message = _Message
        self.LogLevel = _LogLevel
        self.TestID = _TestID
        self.qcResult = _qcResult


qcResults: List[qcMessage] = []


def lambda_handler(event, context):

    # TODO need to rebuild this to use more exception for each type of failure severity
    # TODO exceptions are called from changes that need to be made to the nested if statement below based on outcome of qc checks and settings file
    # VB - exceptions only for hard failures (i.e., errors that cause exceptions, not 'soft' errors) - this is true
    try:
        # @TODO vb - setup, need to add everywhere to replace print statements etc.
        old_stdout = sys.stdout
        new_stdout = io.StringIO()
        sys.stdout = new_stdout

        result = process_prod_table(event, context)

        output = new_stdout.getvalue()
        sys.stdout = old_stdout
        print(output)

        # DQ - log exception returns by send_sns, if any
        functions.send_sns(event, context, output,
                           result)

        functions.create_email(event, context, qcResults, result)

        return event

    except RebuildProdTableException as e:
        logger.debug(e)
        logger.debug("RebuildProdTableException:\ntraceback: "
                     + traceback.format_exc())

        output = new_stdout.getvalue()
        sys.stdout = old_stdout

        logger.debug("RebuildProdTableException:\noutput: "
                     + output)
        if(len(e.args) > 1):
            functions.send_sns(event, context, output,
                               e.args[1])
        else:
            qc_result = config.qc_level.FAILURE_HARD
            functions.send_sns(event, context, output,
                               qc_result)
        raise e

    except Exception as e:
        logger.debug(traceback.format_exc())
        logger.debug(e)
        output = new_stdout.getvalue()
        sys.stdout = old_stdout
        logger.debug(output)
        qc_result = config.qc_level.FAILURE_HARD
        functions.send_sns(event, context, output,
                           qc_result)
        raise e


def process_prod_table(event, context):
    # ...do other things here...
    result = rebuild_prod_table(event, context)
    return result


def rebuild_prod_table(event, context) -> int:
    # Prod table settings
    # file_settings = config.getProdTableSettingsForFileType(event) - now received through event inputs via dynamodb
    logger.debug(event)

    if 'translation_settings' in event:
        # this shouldn't be returning a qc_result value
        qc_result = config.qc_level.SUCCESS_FULL
        qcRes = qcMessage('Debug', "The translation_settings item exists in the config for '{}', meaning this file will be converted into one or more csv files for further processing. These file(s) will then go through the qc_checks, an email detailing the outcome of which should be appearing shortly.".format(
            event["fileType"]), 'Debug')
        qcResults.append(qcRes)
        # Pass through any data files we have setup for conversion to csv.
        # TESTTHIS Does this work and give a good message for unknown filetypes/filetypes with no qc settings in dynamodb?
        print(
            "The translation_settings item exists in the config for '{}', meaning this file will be converted into one or more csv files for further processing. These file(s) will then go through the qc_checks, an email detailing the outcome of which should be appearing shortly.".format(event["fileType"]))
        return qc_result
    elif 'qc_checks' not in event or event['qc_checks'] is None:
        qc_result = config.qc_level.FAILURE_HARD
        # Pass through any registered data files that we don't know about.
        # This means we still need to rebuild these manually.
        # TESTTHIS Does this work and give a good message for unknown filetypes/filetypes with no qc settings in dynamodb?
        qcRes = qcMessage('Debug', "'{}' has no table rebuild logic or quality control checks defined. This dataset will need to be checked and rebuilt manually.".format(
            event["fileType"]), 'Debug')
        qcResults.append(qcRes)
        print(
            "'{}' has no table rebuild logic or quality control checks defined. This dataset will need to be checked and rebuilt manually.".format(event["fileType"]))
        return qc_result

    file_settings = event['qc_checks']
    qcRes = qcMessage('Debug', file_settings, 'Debug')
    qcResults.append(qcRes)

    logger.debug(file_settings)

    # Create table names from our prod table settings
    staging_table_name = "staging_{}".format(file_settings["table_name"])
    prod_table_name = "prod_{}".format(file_settings["table_name"])
    prod_snapshot_table_name = "snapshot_prod_{}_{}".format(file_settings["table_name"], datetime.now(
    ).strftime("%Y_%m_%dt%H_%M_%S"))  # datetime_yyyymmddhhmm_string

    # Templated SQL queries
    # DQ - Pull out SQL queries to separate file
    query_repair_staging_table = "MSCK REPAIR TABLE {database_name}.{staging_table_name}".format(
        database_name=file_settings["database_name"],
        staging_table_name=staging_table_name
    )

    query_staging_table_has_rows = "SELECT COUNT(*) > 0 AS has_rows FROM {database_name}.{staging_table_name}".format(
        database_name=file_settings["database_name"],
        staging_table_name=staging_table_name
    )

    query_snapshot_prod_table = """
            CREATE TABLE IF NOT EXISTS {database_name}.{prod_snapshot_table_name} WITH (
                format = 'PARQUET'
            ) AS
            SELECT * FROM {database_name}.{prod_table_name}
    """.format(
        database_name=file_settings["database_name"],
        prod_snapshot_table_name=prod_snapshot_table_name,
        prod_table_name=prod_table_name
    )

    query_drop_prod_table = "DROP TABLE {database_name}.{prod_table_name}".format(
        database_name=file_settings["database_name"],
        prod_table_name=prod_table_name
    )

    query_rebuild_prod_table = file_settings["create_prod_table_sql"].format(
        database_name=file_settings["database_name"],
        prod_table_name=prod_table_name,
        staging_table_name=staging_table_name
    )

    query_prod_table_has_rows = "SELECT COUNT(*) > 0 AS has_rows FROM {database_name}.{prod_table_name}".format(
        database_name=file_settings["database_name"],
        prod_table_name=prod_table_name
    )

    query_create_prod_from_snapshot = "CREATE TABLE IF NOT EXISTS {database_name}.{prod_table_name} AS SELECT * FROM {database_name}.{prod_snapshot_table_name}".format(
        database_name=file_settings["database_name"],
        prod_table_name=prod_table_name,
        prod_snapshot_table_name=prod_snapshot_table_name
    )

    # Important: This always returns a YYYY-MM-DD formatted date, regardless of what the actual date column has in it (e.g. time) or how it's formatted (e.g. YYYY/MM/DD)
    # Tip: If this query returns an error "Both printing and parsing not supported" that actually means "You provided an empty format string to date_parse".
    # This should only happen in two circumstances:
    # 1. The date_field_name is a varchar but the "date_field_date_format_string" required config parameter is not provided
    # 2. The date_field_name is not of type date, timestamp, or varchar.
    query_date_field_aggregation_query_template = """
        WITH date_format AS (
            SELECT
                CASE typeof({date_field_name})
                    WHEN 'date' THEN '%Y-%m-%d'
                    WHEN 'timestamp' THEN '%Y-%m-%d %H:%i:%S.%f'
                    WHEN 'varchar' THEN '{date_field_date_format_string}'
                    ELSE ''
                END AS date_format_string
            FROM {database_name}.{table_name} LIMIT 1
        ),
        dates_as_string AS (
            SELECT
                try_cast({date_field_name} as varchar) AS date_string
            FROM {database_name}.{table_name}
        )
        SELECT
            {date_field_aggregation_function}(
                CAST(
                    date_parse(
                        date_string,
                        (SELECT date_format_string FROM date_format)
                    ) AS date
                )
            ) FROM dates_as_string
    """

    # Functions to encapsulate logic to execute and handle each of the different queries we use
    # DQ - add in type checking for functions?
    # DQ - move nested functions outside of this function?
    # -> Literal[True]
    def _staging_table_has_rows():
        if config.DEBUG:
            return True
        else:
            try:
                result_snapshot_table_has_rows = _RunSQLQueriesAndGetFirstQueryResult(
                    query_staging_table_has_rows, file_settings["database_name"])

                # String "true" because this query returns a boolean field "has_rows" that we're picking the value of
                if result_snapshot_table_has_rows == "true":
                    qcRes = qcMessage(
                        'Debug', "Test querying the staging table: The table has rows", 'Debug')
                    qcResults.append(qcRes)
                    print("Test querying the staging table: The table has rows")
                    return True
                raise RebuildProdTableException(
                    "Test querying the staging table: The table has no rows")

            except RebuildProdTableException as e:
                raise RebuildProdTableException(
                    "Failed querying the staging table to check it had rows: {}".format(e))

    # -> Literal[True]
    def _snapshot_prod_table():
        if config.DEBUG:
            return True
        else:
            result = _RunSQLQueriesAndGetExecutionStatus(
                query_snapshot_prod_table, file_settings["database_name"])

            if result == "SUCCEEDED":
                qcRes = qcMessage(
                    'Debug',  "Snapshotting prod table: Success", 'Debug')
                qcResults.append(qcRes)
                print("Snapshotting prod table: Success")
                return True
            else:
                raise RebuildProdTableException(
                    "Snapshotting prod table: Failed ({})".format(result))

    # -> Literal[True]
    def _drop_prod_table():
        if config.DEBUG:
            return True
        else:
            result = _RunSQLQueriesAndGetExecutionStatus(
                query_drop_prod_table, file_settings["database_name"])
            if result == "SUCCEEDED":
                qcRes = qcMessage(
                    'Debug',  "Drop prod table: Table dropped", 'Debug')
                qcResults.append(qcRes)
                print("Drop prod table: Table dropped")
                return True
            else:
                raise RebuildProdTableException(
                    "Dropping prod table: Failed ({})".format(result))

    # -> Literal[True]
    def _rebuild_prod_table():
        if config.DEBUG:
            return True
        else:
            result = _RunSQLQueriesAndGetExecutionStatus(
                query_rebuild_prod_table, file_settings["database_name"])

            if result == "SUCCEEDED":
                qcRes = qcMessage(
                    'Debug',  "Rebuild prod table: Table created", 'Debug')
                qcResults.append(qcRes)
                print("Rebuild prod table: Table created")
                return True
            else:
                raise RebuildProdTableException(
                    "Rebuilding prod table: Failed ({})".format(result))

    # -> Literal[True]
    # TODO Phase 2 - Add this into the nested if statement before qc checks are run
    def _prod_has_rows():
        try:
            result_prod_table_has_rows = _RunSQLQueriesAndGetFirstQueryResult(
                query_prod_table_has_rows, file_settings["database_name"])

            # String "true" because this query returns a boolean field "has_rows" that we're picking the value of
            if result_prod_table_has_rows == "true":
                qcRes = qcMessage(
                    'Debug',  "Test prod table has rows: The table has rows", 'Debug')
                qcResults.append(qcRes)
                print("Test prod table has rows: The table has rows")
                return True
            raise RebuildProdTableException(
                "Failed querying the prod table to check it had rows: The table has no rows")

        except RebuildProdTableException as e:
            raise RebuildProdTableException(
                "Failed querying the prod table to check it had rows: {}".format(e))

    # -> Literal[True]
    def _restore_prod_from_snapshot():
        if config.DEBUG:
            return True
        else:
            result = _RunSQLQueriesAndGetExecutionStatus(
                query_create_prod_from_snapshot, file_settings["database_name"])

            if result == "SUCCEEDED":
                qcRes = qcMessage(
                    'Debug',  "Restoring prod from snapshot: Table recreated", 'Debug')
                qcResults.append(qcRes)
                print("Restoring prod from snapshot: Table recreated")
                return True
            else:
                raise RebuildProdTableException(
                    "Restoring prod from snapshot: Failed {}".format(result))

    # -> Literal[True] VB
    def _rollback_prod() -> bool:
        if config.DEBUG:
            return True
        else:
            if _drop_prod_table():
                if _restore_prod_from_snapshot():
                    return True
                else:
                    raise RebuildProdTableException(
                        "Failed restoring prod from snapshot")
            else:
                raise RebuildProdTableException(
                    "Failed dropping prod table (so we could restore it from snapshot)")

    # set data types (like here - variable str and func bool)
    def _runQCChecksOnProdTable(qc_settings: str) -> int:
        # return True
        # add in list of tests to run
        # logger.debug("qc_tests_to_run: " +
        #              str([check_settings['type'] for check_settings in qc_settings]))

        # 0. Report some useful contextual info on the table
        if config.DEBUG:
            # if in debug mode, variables set from config file
            number_of_rows_in_prod_snapshot = config.NUMBER_OF_ROWS_IN_PROD_SNAPSHOT
            number_of_rows_in_prod = config.NUMBER_OF_ROWS_IN_PROD
            number_of_rows_in_staging = config.NUMBER_OF_ROWS_IN_STAGING
        else:
            number_of_rows_in_prod_snapshot = _RunSQLQueriesAndGetQueryResultAsIntegerOrNone(
                "SELECT COUNT(*) AS row_count FROM {database_name}.{prod_snapshot_table_name}".format(
                    database_name=file_settings["database_name"],
                    prod_snapshot_table_name=prod_snapshot_table_name
                ),
                file_settings["database_name"]
            )
            number_of_rows_in_prod = _RunSQLQueriesAndGetQueryResultAsIntegerOrNone(
                "SELECT COUNT(*) AS row_count FROM {database_name}.{prod_table_name}".format(
                    database_name=file_settings["database_name"],
                    prod_table_name=prod_table_name
                ),
                file_settings["database_name"]
            )
            number_of_rows_in_staging = _RunSQLQueriesAndGetQueryResultAsIntegerOrNone(
                "SELECT COUNT(*) AS row_count FROM {database_name}.{staging_table_name}".format(
                    database_name=file_settings["database_name"],
                    staging_table_name=staging_table_name
                ),
                file_settings["database_name"]
            )

        qcRes = qcMessage('Debug', "Number of rows in prod snapshot: {:n}".format(
            number_of_rows_in_prod_snapshot), 'Debug')
        qcResults.append(qcRes)
        qcRes = qcMessage('Debug', "Number of rows in prod: {:n}".format(
            number_of_rows_in_prod), 'Debug')
        qcResults.append(qcRes)
        qcRes = qcMessage('Debug', "Number of rows in staging: {:n}".format(
            number_of_rows_in_staging), 'Debug')
        qcResults.append(qcRes)

        print("QC checks info")
        print("Number of rows in prod snapshot: {:n}".format(
            number_of_rows_in_prod_snapshot))
        print("Number of rows in prod: {:n}".format(number_of_rows_in_prod))
        print("Number of rows in staging: {:n}".format(
            number_of_rows_in_staging))

        # @TODO Validate test settings against a JSON schema
        # @TODO Parameterise all queries to avoid SQL injection vulnerabilities
        # @TODO Unit tests
        # @TODO debug mode to turn on verbose logging, turns off email, bypasses failure on things like MSCK (due to partitioning etc.)

        # set up our function dictionary
        qc_check_functions = {}

        # this is a parametered decorator
        def register_qc_check(check_type):
            # this is the actual decorator
            def registered_qc_check(func):
                qc_check_functions[check_type] = func
                return func
            return registered_qc_check

        # 0. SNIFF TEST: THE NUMBER OF ROWS IN PROD HASN'T CHANGED BY MORE THAN [N] VS THE PROD SNAPSHOT
        @register_qc_check("prod_record_count_change_vs_snapshot_is_within_limits")
        def _prod_record_count_change_vs_snapshot_is_within_limits(settings):
            # TODO: only create qcRes and qcResults if test is actually run!!!
            print("Test prod table record count change is within limits: Begin")
            print("purpose: {}".format(settings["purpose"]))
            print("direction: {}".format(settings["direction"]))
            print("limit: {}".format(settings["limit"]))
            TestID = hashlib.md5(pickle.dumps(settings)).hexdigest()
            qcRes = qcMessage(
                'Test_Messages', "Test prod table record count change is within limits: Begin", 'UserEmail', TestID)
            qcResults.append(qcRes)  # integrate append with creation?
            qcRes = qcMessage('Test_Purpose', "{}".format(
                settings["purpose"]), 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage('Test_Messages', "direction: {}".format(
                settings["direction"]), 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage(
                'Test_Messages', "limit: {}".format(settings["limit"]), 'UserEmail', TestID)
            qcResults.append(qcRes)

            delta = number_of_rows_in_prod - number_of_rows_in_prod_snapshot
            qcRes = qcMessage('Test_Messages', "delta: {}".format(
                delta), 'UserEmail', TestID)
            qcResults.append(qcRes)
            print("delta: {}".format(delta))

            # The number of records should not have increased by more than [n]
            if settings["direction"] == "INCREASE":
                if delta > settings["limit"]:
                    print("Test prod table record count change is within limits: FAILURE_HARD. The number of rows in prod has increased by {delta}, which is over the threshold of {limit}.".format(
                        delta=delta, limit=settings["limit"]))
                    qc_result = config.qc_level.FAILURE_HARD
                    qcRes = qcMessage('Test_Summary_Message', "Test prod table record count change is within limits: {qc_result}. The number of rows in prod has increased by {delta}, which is over the threshold of {limit}.".format(
                        qc_result=qc_result.name, delta=delta, limit=settings["limit"]), 'UserEmail', TestID)
                    qcResults.append(qcRes)
                    qcRes = qcMessage('Test_QC_Result', '',
                                      'UserEmail', TestID, qc_result)
                    qcResults.append(qcRes)
                    return qc_result  # TODO: move this to end of if statement so it's in one place

            elif settings["direction"] == "DECREASE":
                if delta < 0 and abs(delta) > settings["limit"]:
                    print("Test prod table record count change is within limits: FAILURE_HARD. The number of rows in prod has decreased by {delta}, which is over the threshold of {limit}.".format(
                        delta=abs(delta), limit=settings["limit"]))
                    qc_result = config.qc_level.FAILURE_HARD
                    qcRes = qcMessage('Test_Summary_Message', "Test prod table record count change is within limits: {qc_result}. The number of rows in prod has decreased by {delta}, which is over the threshold of {limit}.".format(
                        qc_result=qc_result.name, delta=abs(delta), limit=settings["limit"]), 'UserEmail', TestID)
                    qcResults.append(qcRes)
                    qcRes = qcMessage('Test_QC_Result', '',
                                      'UserEmail', TestID, qc_result)
                    qcResults.append(qcRes)
                    return qc_result

            # @TODO Assumes the settings have been validated
            qc_result = config.qc_level.SUCCESS_FULL
            qcRes = qcMessage('Test_Summary_Message', "Test prod table record count change is within limits: {qc_result}. The number of rows in prod has changed by {delta}, which is within the threshold of a {direction} of no more than {limit}.".format(
                qc_result=qc_result.name, delta=delta, direction=settings["direction"], limit=settings["limit"]), 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage('Test_QC_Result', '',
                              'UserEmail', TestID, qc_result)
            qcResults.append(qcRes)
            print("Test prod table record count change is within limits: SUCCESS_FULL. The number of rows in prod has changed by {delta}, which is within the threshold of a {direction} of no more than {limit}.".format(
                delta=delta, direction=settings["direction"], limit=settings["limit"]))
            return qc_result  # TODO: change so there's only one return per function

        # 1. SNIFF TEST: THE NUMBER OF ROWS IN PROD HASN'T CHANGED BY MORE THAN [N]% - [N]% FOR ANY GIVEN DAY/MONTH
        @register_qc_check("prod_record_count_change_is_within_limits")
        def _prod_record_count_change_is_within_limits(settings):
            TestID = hashlib.md5(pickle.dumps(settings)).hexdigest()
            qcRes = qcMessage('Test_Messages',
                              "# 1. SNIFF TEST: THE NUMBER OF ROWS IN PROD HASN'T CHANGED BY MORE THAN [N]% - [N]% FOR ANY GIVEN DAY/MONTH", 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage(
                'Test_Messages', "Test prod table record change is within limits: Begin", 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage('Test_Purpose', "{}".format(
                settings["purpose"]), 'UserEmail', TestID)
            qcResults.append(qcRes)

            print(
                "# 1. SNIFF TEST: THE NUMBER OF ROWS IN PROD HASN'T CHANGED BY MORE THAN [N]% - [N]% FOR ANY GIVEN DAY/MONTH")
            print("Test prod table record change is within limits: Begin")
            print("purpose: {}".format(settings["purpose"]))
            values_in_data_table = _RunSQLQueriesAndGetQueryResultsFromColumn(
                "SELECT {field_name} FROM {database_name}.{table_name}".format(
                    field_name=settings["field_name"],
                    table_name=settings["table_name"],
                    database_name=settings["database_name"]
                ),
                settings["database_name"],
                settings["field_name"]
            )
            qcRes = qcMessage(
                'Test_Messages', "values_in_data_table (list): {}".format(values_in_data_table), 'UserEmail', TestID)
            qcResults.append(qcRes)
            print("values_in_data_table (list): {}".format(values_in_data_table))
            try:
                values_in_data_table_to_int = [
                    int(x) for x in values_in_data_table]

            except RebuildProdTableException as e:
                logger.debug(
                    "Couldn't convert all values in values_in_data_table to int")
                logger.debug(values_in_data_table)
                logger.debug(e)

            values_in_data_table_average = sum(
                values_in_data_table_to_int)/len(values_in_data_table_to_int)

            values_in_data_table_percentage_difference = []
            for row_value in values_in_data_table_to_int:
                values_in_data_table_percentage_difference.append(abs(
                    row_value-values_in_data_table_average)/((row_value+values_in_data_table_average)/2)*100)
            print("values_in_data_table_percentage_difference (list): {}".format(
                values_in_data_table_percentage_difference))
            qcRes = qcMessage('Test_Messages', "values_in_data_table_percentage_difference (list): {}".format(
                values_in_data_table_percentage_difference), 'UserEmail', TestID)
            qcResults.append(qcRes)
            qc_result = config.qc_level(
                max([severity.value for severity in config.qc_level]))
            for severity in config.qc_level:
                if severity.name in settings['failure_threshold']:
                    # If our lower bound is less than or equal to the largest % difference
                    # AND
                    # if our largest % difference is less than or equal to our upper bound is
                    # Then we meet this criteria, so set the severity level who's checks matched, and break the loop
                    if settings['failure_threshold'][severity.name]["change_lower_bound"] <= max(values_in_data_table_percentage_difference) <= settings["failure_threshold"][severity.name]["change_upper_bound"]:
                        qc_result = severity
                        number_of_months_above_threshold = sum(
                            i > settings["failure_threshold"][severity.name]["change_lower_bound"] for i in values_in_data_table_percentage_difference)
                        break

            try:
                # The number of records should not have increased by more than [n]%
                # make sure it works when you only have one level of record count change you care about.
                # include purpose in messages from config for each level if called.
                if qc_result == config.qc_level.SUCCESS_FULL:
                    qcRes = qcMessage('Test_Summary_Message', "All months in the dataset are within the accepted threshold range of no more than a {lowerbound}% - {upperbound}% change in the number of records per month.".format(
                        lowerbound=settings["failure_threshold"][qc_result.name]["change_lower_bound"], upperbound=settings["failure_threshold"][qc_result.name]["change_upper_bound"]), 'UserEmail', TestID)
                    qcResults.append(qcRes)
                    print("All months in the dataset are within the accepted threshold range of no more than a {lowerbound}% - {upperbound}% change in the number of records per month.".format(
                        lowerbound=settings["failure_threshold"][qc_result.name]["change_lower_bound"], upperbound=settings["failure_threshold"][qc_result.name]["change_upper_bound"]))
                elif qc_result.name in settings['failure_threshold']:
                    qcRes = qcMessage('Test_Summary_Message',
                                      "There are {monthsabovethreshold} month(s) in the dataset that have fallen within the threshold range of {lowerbound}% - {upperbound}% change in the number of records per month".format(
                                          monthsabovethreshold=number_of_months_above_threshold, lowerbound=settings["failure_threshold"][qc_result.name]["change_lower_bound"], upperbound=settings["failure_threshold"][qc_result.name]["change_upper_bound"]), 'UserEmail', TestID)
                    qcResults.append(qcRes)
                    print(
                        "There are {monthsabovethreshold} month(s) in the dataset that have fallen within the threshold range of {lowerbound}% - {upperbound}% change in the number of records per month".format(
                            monthsabovethreshold=number_of_months_above_threshold, lowerbound=settings["failure_threshold"][qc_result.name]["change_lower_bound"], upperbound=settings["failure_threshold"][qc_result.name]["change_upper_bound"]))
                else:
                    # This means we've "fallen through" the config range provided i.e. The change is outside all of the bounds provided in the config. Add in
                    qcRes = qcMessage('Test_Summary_Message',
                                      "Test prod table record count change is not within limits. One or more have exceeded all threshold ranges for the change in the number of records per month", 'UserEmail', TestID)
                    qcResults.append(qcRes)
                    print(
                        "Test prod table record count change is not within limits. One or more have exceeded all threshold ranges for the change in the number of records per month")
                    logger.info(values_in_data_table_percentage_difference)
                print(qc_result.name)
                qcRes = qcMessage('Test_QC_Result', '',
                                  'UserEmail', TestID, qc_result)
                qcResults.append(qcRes)
                return qc_result
            except RebuildProdTableException:
                qcRes = qcMessage('Debug',
                                  "Encountered an unknown 'prod_has_at_least_one_row' configuration", 'Debug', TestID)
                qcResults.append(qcRes)

            # @TODO Assumes the settings have been validated

        # 2. SNIFF TEST: A DATE FIELD IS NOT LATER THAN A GIVEN DATE

        @register_qc_check("prod_date_field_is_not_later_than_date")
        def _prod_date_field_is_not_later_than_date(settings):
            TestID = hashlib.md5(pickle.dumps(settings)).hexdigest()
            print(
                "# 2. SNIFF TEST: A DATE FIELD IS NOT LATER THAN A GIVEN DATE")
            print("Test prod table date is not later than date: Begin")
            print("purpose: {}".format(settings["purpose"]))
            print("date_field_name: {}".format(settings["date_field_name"]))
            print("date_field_aggregation_function: {}".format(
                settings["date_field_aggregation_function"]))
            print("date_field_date_format_string: {}".format(
                settings["date_field_date_format_string"] if "date_field_date_format_string" in settings else None))
            print("date_not_later_than: {}".format(
                settings["date_not_later_than"]))
            qcRes = qcMessage('Test_Messages',
                              "# 2. SNIFF TEST: A DATE FIELD IS NOT LATER THAN A GIVEN DATE", 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage(
                'Test_Messages', "Test prod table date is not later than date: Begin", 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage('Test_Purpose', "{}".format(
                settings["purpose"]), 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage('Test_Messages', "date_field_name: {}".format(
                settings["date_field_name"]), 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage('Test_Messages', "date_field_aggregation_function: {}".format(
                settings["date_field_aggregation_function"]), 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage('Test_Messages', "date_field_date_format_string: {}".format(
                settings["date_field_date_format_string"] if "date_field_date_format_string" in settings else None), 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage('Test_Messages', "date_not_later_than: {}".format(
                settings["date_not_later_than"]), 'UserEmail', TestID)
            qcResults.append(qcRes)

            if isinstance(settings["date_not_later_than"], str):
                date_not_later_than = datetime.strptime(
                    settings["date_not_later_than"], "%Y-%m-%d").date()
            elif isinstance(settings["date_not_later_than"], dict) and "days_before_current_day" in settings["date_not_later_than"]:
                date_not_later_than = date.today(
                ) - timedelta(days=settings["date_not_later_than"]["days_before_current_day"])
            elif isinstance(settings["date_not_later_than"], dict) and "days_after_current_day" in settings["date_not_later_than"]:
                date_not_later_than = date.today(
                ) + timedelta(days=settings["date_not_later_than"]["days_after_current_day"])
            else:
                raise RebuildProdTableException(
                    "Encountered an unknown 'date_not_later_than' configuration")
            print("date_not_later_than (object): {}".format(date_not_later_than))
            qcRes = qcMessage('Test_Messages', "date_not_later_than (object): {}".format(
                date_not_later_than), 'UserEmail', TestID)
            qcResults.append(qcRes)

            date_field_string = _RunSQLQueriesAndGetFirstQueryResult(
                query_date_field_aggregation_query_template.format(
                    date_field_aggregation_function=settings["date_field_aggregation_function"],
                    date_field_name=settings["date_field_name"],
                    date_field_date_format_string=settings[
                        "date_field_date_format_string"] if "date_field_date_format_string" in settings else "",
                    database_name=file_settings["database_name"],
                    table_name=prod_table_name
                ),
                file_settings["database_name"]
            )
            print("date_field (string): {}".format(date_field_string))

            qcRes = qcMessage('Test_Messages', "date_field (string): {}".format(
                date_field_string), 'UserEmail', TestID)
            qcResults.append(qcRes)
            date_field_object = datetime.strptime(
                date_field_string, "%Y-%m-%d").date()
            qcRes = qcMessage('Test_Messages', "date_field (object): {}".format(
                date_field_object), 'UserEmail', TestID)
            qcResults.append(qcRes)
            print("date_field (object): {}".format(date_field_object))

            if date_field_object > date_not_later_than:
                print("Test prod table date is not later than date: FAILURE_HARD. The {date_field_aggregation_function} of '{date_field_name}' is {date_field_object}, which is later than the threshold date of {date_not_later_than}".format(
                    date_field_aggregation_function=settings["date_field_aggregation_function"], date_field_name=settings["date_field_name"], date_field_object=date_field_object, date_not_later_than=date_not_later_than))
                qc_result = config.qc_level.FAILURE_HARD
                qcRes = qcMessage('Test_Summary_Message', "Test prod table date is not later than date: {qc_result}. The {date_field_aggregation_function} of '{date_field_name}' is {date_field_object}, which is later than the threshold date of {date_not_later_than}".format(
                    qc_result=qc_result.name, date_field_aggregation_function=settings["date_field_aggregation_function"], date_field_name=settings["date_field_name"], date_field_object=date_field_object, date_not_later_than=date_not_later_than), 'UserEmail', TestID)
                qcResults.append(qcRes)
                # VB - example of place you'd add in more checks for more success modes
                qcRes = qcMessage('Test_QC_Result', '',
                                  'UserEmail', TestID, qc_result)
                qcResults.append(qcRes)
                return qc_result

            qc_result = config.qc_level.SUCCESS_FULL
            qcRes = qcMessage('Test_Summary_Message', "Test prod table date is not later than date: {qc_result}. The {date_field_aggregation_function} of '{date_field_name}' is {date_field_object}, which is before or on the threshold date of {date_not_later_than}".format(
                qc_result=qc_result.name, date_field_aggregation_function=settings["date_field_aggregation_function"], date_field_name=settings["date_field_name"], date_field_object=date_field_object, date_not_later_than=date_not_later_than), 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage('Test_QC_Result', '',
                              'UserEmail', TestID, qc_result)
            qcResults.append(qcRes)
            print("Test prod table date is not later than date: SUCCESS_FULL. The {date_field_aggregation_function} of '{date_field_name}' is {date_field_object}, which is before or on the threshold date of {date_not_later_than}".format(
                date_field_aggregation_function=settings["date_field_aggregation_function"], date_field_name=settings["date_field_name"], date_field_object=date_field_object, date_not_later_than=date_not_later_than))
            return qc_result

        # 3. SNIFF TEST: A DATE FIELD IS NOT BEFORE A GIVEN DATE

        @register_qc_check("prod_date_field_is_not_before_date")
        def _prod_date_field_is_not_before_date(settings):
            TestID = hashlib.md5(pickle.dumps(settings)).hexdigest()
            print(
                "# 3. SNIFF TEST: A DATE FIELD IS NOT BEFORE A GIVEN DATE")
            print("Test prod table date is not earlier than date: Begin")
            print("purpose: {}".format(settings["purpose"]))
            print("date_field_name: {}".format(settings["date_field_name"]))
            print("date_field_aggregation_function: {}".format(
                settings["date_field_aggregation_function"]))
            print("date_field_date_format_string: {}".format(
                settings["date_field_date_format_string"] if "date_field_date_format_string" in settings else None))
            print("date_not_earlier_than: {}".format(
                settings["date_not_earlier_than"]))

            qcRes = qcMessage('Test_Messages',
                              "# 3. SNIFF TEST: A DATE FIELD IS NOT BEFORE A GIVEN DATE", 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage(
                'Test_Messages', "Test prod table date is not earlier than date: Begin", 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage('Test_Purpose', "{}".format(
                settings["purpose"]), 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage('Test_Messages', "date_field_name: {}".format(
                settings["date_field_name"]), 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage('Test_Messages', "date_field_aggregation_function: {}".format(
                settings["date_field_aggregation_function"]), 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage('Test_Messages', "date_field_date_format_string: {}".format(
                settings["date_field_date_format_string"] if "date_field_date_format_string" in settings else None), 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage('Test_Messages', "date_not_earlier_than: {}".format(
                settings["date_not_earlier_than"]), 'UserEmail', TestID)
            qcResults.append(qcRes)

            if isinstance(settings["date_not_earlier_than"], str):
                date_not_earlier_than = datetime.strptime(
                    settings["date_not_earlier_than"], "%Y-%m-%d").date()
            elif isinstance(settings["date_not_earlier_than"], dict) and "days_before_current_day" in settings["date_not_earlier_than"]:
                date_not_earlier_than = date.today(
                ) - timedelta(days=settings["date_not_earlier_than"]["days_before_current_day"])
            elif isinstance(settings["date_not_earlier_than"], dict) and "days_after_current_day" in settings["date_not_earlier_than"]:
                date_not_earlier_than = date.today(
                ) + timedelta(days=settings["date_not_earlier_than"]["days_after_current_day"])
            else:
                raise RebuildProdTableException(
                    "Encountered an unknown 'date_not_earlier_than' configuration")
            print("date_not_earlier_than (object): {}".format(
                date_not_earlier_than))
            qcRes = qcMessage('Test_Messages', "date_not_earlier_than (object): {}".format(
                date_not_earlier_than), 'UserEmail', TestID)
            qcResults.append(qcRes)

            date_field_string = _RunSQLQueriesAndGetFirstQueryResult(
                query_date_field_aggregation_query_template.format(
                    date_field_aggregation_function=settings["date_field_aggregation_function"],
                    date_field_name=settings["date_field_name"],
                    date_field_date_format_string=settings[
                        "date_field_date_format_string"] if "date_field_date_format_string" in settings else "",
                    database_name=file_settings["database_name"],
                    table_name=prod_table_name
                ),
                file_settings["database_name"]
            )
            print("date_field (string): {}".format(date_field_string))

            qcRes = qcMessage('Test_Messages', "date_field (string): {}".format(
                date_field_string), 'UserEmail', TestID)
            qcResults.append(qcRes)

            date_field_object = datetime.strptime(
                date_field_string, "%Y-%m-%d").date()
            print("date_field (object): {}".format(date_field_object))
            qcRes = qcMessage('Test_Messages', "date_field (object): {}".format(
                date_field_object), 'UserEmail', TestID)
            qcResults.append(qcRes)

            if date_field_object < date_not_earlier_than:
                qc_result = config.qc_level.FAILURE_HARD
                qcRes = qcMessage('Test_Summary_Message', "Test prod table date is not earlier than date: {qc_result}. The {date_field_aggregation_function} of '{date_field_name}' is {date_field_object}, which is earlier than the threshold date of {date_not_earlier_than}".format(
                    qc_result=qc_result.name, date_field_aggregation_function=settings["date_field_aggregation_function"], date_field_name=settings["date_field_name"], date_field_object=date_field_object, date_not_earlier_than=date_not_earlier_than), 'UserEmail', TestID)
                qcResults.append(qcRes)
                qcRes = qcMessage('Test_QC_Result', '',
                                  'UserEmail', TestID, qc_result)
                qcResults.append(qcRes)
                print("Test prod table date is not earlier than date: FAILURE_HARD. The {date_field_aggregation_function} of '{date_field_name}' is {date_field_object}, which is earlier than the threshold date of {date_not_earlier_than}".format(
                    date_field_aggregation_function=settings["date_field_aggregation_function"], date_field_name=settings["date_field_name"], date_field_object=date_field_object, date_not_earlier_than=date_not_earlier_than))
                print(config.qc_level.FAILURE_HARD.name)
                return qc_result

            qc_result = config.qc_level.SUCCESS_FULL
            qcRes = qcMessage('Test_Summary_Message', "Test prod table date is not earlier than date: {qc_result}. The {date_field_aggregation_function} of '{date_field_name}' is {date_field_object}, which is on or after the threshold date of {date_not_earlier_than}".format(
                qc_result=qc_result.name, date_field_aggregation_function=settings["date_field_aggregation_function"], date_field_name=settings["date_field_name"], date_field_object=date_field_object, date_not_earlier_than=date_not_earlier_than), 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage('Test_QC_Result', '',
                              'UserEmail', TestID, qc_result)
            qcResults.append(qcRes)
            print("Test prod table date is not earlier than date: SUCCESS_FULL. The {date_field_aggregation_function} of '{date_field_name}' is {date_field_object}, which is on or after the threshold date of {date_not_earlier_than}".format(
                date_field_aggregation_function=settings["date_field_aggregation_function"], date_field_name=settings["date_field_name"], date_field_object=date_field_object, date_not_earlier_than=date_not_earlier_than))
            print(config.qc_level.SUCCESS_FULL.name)
            return qc_result

        # 3A. SNIFF TEST: A DATE FIELD IS NOT OUTSIDE A GIVEN DATE RANGE

        @register_qc_check("prod_date_field_is_not_outside_expected_date_range")
        def _prod_date_field_is_not_outside_expected_date_range(settings):
            TestID = hashlib.md5(pickle.dumps(settings)).hexdigest()
            logger.debug(
                "# 3A. SNIFF TEST: A DATE FIELD IS NOT OUTSIDE A GIVEN DATE RANGE")
            print(
                "# 3A. SNIFF TEST: A DATE FIELD IS NOT OUTSIDE A GIVEN DATE RANGE")
            print("Test prod table date is outside expected date range: Begin")
            print("purpose: {}".format(settings["purpose"]))
            print("date_field_name: {}".format(settings["date_field_name"]))
            print("date_field_aggregation_function: {}".format(
                settings["date_field_aggregation_function"]))
            print("date_field_date_format_string: {}".format(
                settings["date_field_date_format_string"] if "date_field_date_format_string" in settings else None))
            print("date_not_earlier_than: {}".format(
                settings["date_not_earlier_than"]))
            qcRes = qcMessage('Test_Messages',
                              "# 3A. SNIFF TEST: A DATE FIELD IS NOT OUTSIDE A GIVEN DATE RANGE", 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage(
                'Test_Messages', "Test prod table date is outside expected date range: Begin", 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage('Test_Purpose', "{}".format(
                settings["purpose"]), 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage('Test_Messages', "date_field_name: {}".format(
                settings["date_field_name"]), 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage('Test_Messages', "date_field_aggregation_function: {}".format(
                settings["date_field_aggregation_function"]), 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage('Test_Messages', "date_field_date_format_string: {}".format(
                settings["date_field_date_format_string"] if "date_field_date_format_string" in settings else None), 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage('Test_Messages', "date_not_earlier_than: {}".format(
                settings["date_not_earlier_than"]), 'UserEmail', TestID)
            qcResults.append(qcRes)

            if isinstance(settings["date_not_earlier_than"], str):
                date_not_earlier_than = datetime.strptime(
                    settings["date_not_earlier_than"], "%Y-%m-%d").date()
            elif isinstance(settings["date_not_earlier_than"], dict) and "days_before_current_day" in settings["date_not_earlier_than"]:
                date_not_earlier_than = date.today(
                ) - timedelta(days=settings["date_not_earlier_than"]["days_before_current_day"])
            elif isinstance(settings["date_not_earlier_than"], dict) and "days_after_current_day" in settings["date_not_earlier_than"]:
                date_not_earlier_than = date.today(
                ) + timedelta(days=settings["date_not_earlier_than"]["days_after_current_day"])
            else:
                raise RebuildProdTableException(
                    "Encountered an unknown 'date_not_earlier_than' configuration")
            qcRes = qcMessage('Test_Messages', "date_not_earlier_than (object): {}".format(
                date_not_earlier_than), 'UserEmail', TestID)
            qcResults.append(qcRes)
            print("date_not_earlier_than (object): {}".format(
                date_not_earlier_than))

            date_field_string = _RunSQLQueriesAndGetFirstQueryResult(
                query_date_field_aggregation_query_template.format(
                    date_field_aggregation_function=settings["date_field_aggregation_function"],
                    date_field_name=settings["date_field_name"],
                    date_field_date_format_string=settings[
                        "date_field_date_format_string"] if "date_field_date_format_string" in settings else "",
                    database_name=file_settings["database_name"],
                    table_name=prod_table_name
                ),
                file_settings["database_name"]
            )
            print("date_field (string): {}".format(date_field_string))

            qcRes = qcMessage('Test_Messages', "date_field (string): {}".format(
                date_field_string), 'UserEmail', TestID)
            qcResults.append(qcRes)

            date_field_object = datetime.strptime(
                date_field_string, "%Y-%m-%d").date()
            qcRes = qcMessage('Test_Messages', "date_field (object): {}".format(
                date_field_object), 'UserEmail', TestID)
            qcResults.append(qcRes)
            print("date_field (object): {}".format(date_field_object))

            if date_field_object < date_not_earlier_than:
                qc_result = config.qc_level.FAILURE_HARD
                qcRes = qcMessage('Test_Summary_Message', "Test prod table date is not earlier than date: {qc_result}. The {date_field_aggregation_function} of '{date_field_name}' is {date_field_object}, which is earlier than the threshold date of {date_not_earlier_than}".format(
                    qc_result=qc_result.name, date_field_aggregation_function=settings["date_field_aggregation_function"], date_field_name=settings["date_field_name"], date_field_object=date_field_object, date_not_earlier_than=date_not_earlier_than), 'UserEmail', TestID)
                qcResults.append(qcRes)
                qcRes = qcMessage('Test_QC_Result', '',
                                  'UserEmail', TestID, qc_result)
                qcResults.append(qcRes)
                print("Test prod table date is not earlier than date: FAILURE_HARD. The {date_field_aggregation_function} of '{date_field_name}' is {date_field_object}, which is earlier than the threshold date of {date_not_earlier_than}".format(
                    date_field_aggregation_function=settings["date_field_aggregation_function"], date_field_name=settings["date_field_name"], date_field_object=date_field_object, date_not_earlier_than=date_not_earlier_than))
                return qc_result

            qc_result = config.qc_level.SUCCESS_FULL
            qcRes = qcMessage('Test_Summary_Message', "Test prod table date is not earlier than date: {qc_result}. The {date_field_aggregation_function} of '{date_field_name}' is {date_field_object}, which is on or after the threshold date of {date_not_earlier_than}".format(
                qc_result=qc_result.name, date_field_aggregation_function=settings["date_field_aggregation_function"], date_field_name=settings["date_field_name"], date_field_object=date_field_object, date_not_earlier_than=date_not_earlier_than), 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage('Test_QC_Result', '',
                              'UserEmail', TestID, qc_result)
            qcResults.append(qcRes)
            print("Test prod table date is not earlier than date: SUCCESS_FULL. The {date_field_aggregation_function} of '{date_field_name}' is {date_field_object}, which is on or after the threshold date of {date_not_earlier_than}".format(
                date_field_aggregation_function=settings["date_field_aggregation_function"], date_field_name=settings["date_field_name"], date_field_object=date_field_object, date_not_earlier_than=date_not_earlier_than))
            return qc_result

        # 4. SNIFF TEST: A DATE FIELD IS WITHIN LIMITS BY CURRENT DAY OF WEEK

        @register_qc_check("prod_date_field_should_be_n_days_ago")
        def _prod_date_field_should_be_n_days_ago(settings):
            TestID = hashlib.md5(pickle.dumps(settings)).hexdigest()

            logger.debug(
                "# 4. SNIFF TEST: A DATE FIELD IS WITHIN LIMITS BY CURRENT DAY OF WEEK")
            qcRes = qcMessage('Test_Messages',
                              "# 4. SNIFF TEST: A DATE FIELD IS WITHIN LIMITS BY CURRENT DAY OF WEEK", 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage(
                'Test_Messages', "Test prod table date is within limits: Begin", 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage('Test_Purpose', "{}".format(
                settings["purpose"]), 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage('Test_Messages', "date_field_name: {}".format(
                settings["date_field_name"]), 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage('Test_Messages', "date_field_aggregation_function: {}".format(
                settings["date_field_aggregation_function"]), 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage('Test_Messages', "date_field_date_format_string: {}".format(
                settings["date_field_date_format_string"] if "date_field_date_format_string" in settings else None), 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage('Test_Messages', "use_staging_table: {}".format(
                settings["use_staging_table"] if "use_staging_table" in settings else None), 'UserEmail', TestID)
            qcResults.append(qcRes)
            print(
                "# 4. SNIFF TEST: A DATE FIELD IS WITHIN LIMITS BY CURRENT DAY OF WEEK")
            print("Test prod table date is within limits: Begin")
            print("purpose: {}".format(settings["purpose"]))
            print("date_field_name: {}".format(settings["date_field_name"]))
            print("date_field_aggregation_function: {}".format(
                settings["date_field_aggregation_function"]))
            print("date_field_date_format_string: {}".format(
                settings["date_field_date_format_string"] if "date_field_date_format_string" in settings else None))
            print("use_staging_table: {}".format(
                settings["use_staging_table"] if "use_staging_table" in settings else None))
            current_weekday_name = date.today().strftime("%A").upper()

            try:
                # Get most recent date in datafile
                date_field_string = _RunSQLQueriesAndGetFirstQueryResult(
                    query_date_field_aggregation_query_template.format(
                        date_field_aggregation_function=settings["date_field_aggregation_function"],
                        date_field_name=settings["date_field_name"],
                        date_field_date_format_string=settings[
                            "date_field_date_format_string"] if "date_field_date_format_string" in settings else "",
                        database_name=file_settings["database_name"],
                        table_name=staging_table_name if "use_staging_table" in settings and settings[
                            "use_staging_table"] is True else prod_table_name
                    ),
                    file_settings["database_name"]
                )
                print("date_field (string): {}".format(date_field_string))

                qcRes = qcMessage('Test_Messages', "date_field (string): {}".format(
                    date_field_string), 'UserEmail', TestID)
                qcResults.append(qcRes)

                date_field_object = datetime.strptime(
                    date_field_string, "%Y-%m-%d").date()
                print("date_field (object): {}".format(date_field_object))

                qcRes = qcMessage('Test_Messages', "date_field (object): {}".format(
                    date_field_object), 'UserEmail', TestID)
                qcResults.append(qcRes)

                qc_result = config.qc_level(
                    max([severity.value for severity in config.qc_level]))
                for severity in config.qc_level:
                    if severity.name in settings['failure_threshold']:
                        qcRes = qcMessage('Test_Messages', "date_should_be_n_days_ago: {}".format(
                            settings['failure_threshold'][severity.name]['date_should_be_n_days_ago']), 'UserEmail', TestID)
                        qcResults.append(qcRes)
                        print("date_should_be_n_days_ago: {}".format(
                            settings['failure_threshold'][severity.name]['date_should_be_n_days_ago']))
                        if isinstance(settings['failure_threshold'][severity.name]['date_should_be_n_days_ago'], int):
                            number_of_days_to_subtract = settings['failure_threshold'][
                                severity.name]['date_should_be_n_days_ago']
                        elif isinstance(settings['failure_threshold'][severity.name]['date_should_be_n_days_ago'], dict):
                            print("current_weekday_name: {}".format(
                                current_weekday_name))
                            qcRes = qcMessage('Test_Messages', "current_weekday_name: {}".format(
                                current_weekday_name), 'UserEmail', TestID)
                            qcResults.append(qcRes)
                            number_of_days_to_subtract = settings['failure_threshold'][
                                severity.name]['date_should_be_n_days_ago'][current_weekday_name]
                        print("number_of_days_to_subtract: {}".format(
                            number_of_days_to_subtract))
                        qcRes = qcMessage('Test_Messages', "number_of_days_to_subtract: {}".format(
                            number_of_days_to_subtract), 'UserEmail', TestID)
                        qcResults.append(qcRes)

                        date_should_be = date.today() - timedelta(days=number_of_days_to_subtract)
                        print("date_should_be (object): {}".format(date_should_be))
                        qcRes = qcMessage('Test_Messages', "date_should_be (object): {}".format(
                            date_should_be), 'UserEmail', TestID)
                        qcResults.append(qcRes)

                        date_diff = abs(
                            (date_should_be - date_field_object).days)
                        if date_diff == 0:
                            qc_result = severity
                            break

                if qc_result == config.qc_level.FAILURE_HARD:
                    print("Test prod table date is not within any of the limits required to pass this check and has a status of: {severity_level}. The {date_field_aggregation_function} of '{date_field_name}' field is {date_field_object}, which differs from the expected date of {date_should_be} by more than any of the threshold values set for this filetype".format(
                        severity_level=qc_result.name, date_field_aggregation_function=settings["date_field_aggregation_function"], date_field_name=settings["date_field_name"], date_field_object=date_field_object, date_should_be=date_should_be))
                    qcRes = qcMessage('Test_Summary_Message', "Test prod table date is not within any of the limits required to pass this check and has a status of: {severity_level}. The {date_field_aggregation_function} of '{date_field_name}' field is {date_field_object}, which differs from the expected date of {date_should_be} by more than any of the threshold values set for this filetype".format(
                        severity_level=qc_result.name, date_field_aggregation_function=settings["date_field_aggregation_function"], date_field_name=settings["date_field_name"], date_field_object=date_field_object, date_should_be=date_should_be), 'UserEmail', TestID)
                    qcResults.append(qcRes)
                else:
                    print("Test prod table date is within the threshold values required for a status of: {severity_level}. The {date_field_aggregation_function} of '{date_field_name}' field is {date_field_object} and the threshold date for this severity level is {date_should_be}".format(
                        severity_level=qc_result.name, date_field_aggregation_function=settings["date_field_aggregation_function"], date_field_name=settings["date_field_name"], date_field_object=date_field_object, date_should_be=date_should_be))
                    qcRes = qcMessage('Test_Summary_Message', "Test prod table date is within the threshold values required for a status of: {severity_level}. The {date_field_aggregation_function} of '{date_field_name}' field is {date_field_object} and the threshold date for this severity level is {date_should_be}".format(
                        severity_level=qc_result.name, date_field_aggregation_function=settings["date_field_aggregation_function"], date_field_name=settings["date_field_name"], date_field_object=date_field_object, date_should_be=date_should_be), 'UserEmail', TestID)
                    qcResults.append(qcRes)
                qcRes = qcMessage('Test_QC_Result', '',
                                  'UserEmail', TestID, qc_result)
                qcResults.append(qcRes)
                return qc_result

            except RebuildProdTableException as e:
                raise RebuildProdTableException(
                    "Encountered an unknown 'date_should_be_n_days_ago' configuration error {}:".format(e))

            # TODO - Create validation for settings incoming and make sure they are valid, the right datatypes, mandatory variables are included, etc.

        # 5. SNIFF TEST: DOES THE NEW PROD TABLE HAVE ANY ROWS?

        @register_qc_check("prod_has_at_least_one_row")
        def _prod_has_at_least_one_row(settings):
            TestID = hashlib.md5(pickle.dumps(settings)).hexdigest()
            logger.debug(
                "# 5. SNIFF TEST: DOES THE NEW PROD TABLE HAVE ANY ROWS?")
            qcRes = qcMessage('Test_Messages',
                              "# 5. SNIFF TEST: DOES THE NEW PROD TABLE HAVE ANY ROWS?", 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage(
                'Test_Messages', "Test prod table has at least one row: Begin", 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage('Test_Purpose', "{}".format(
                settings["purpose"]), 'UserEmail', TestID)
            qcResults.append(qcRes)
            print(
                "# 5. SNIFF TEST: DOES THE NEW PROD TABLE HAVE ANY ROWS?")
            print("Test prod table has at least one row: Begin")
            print("purpose: {}".format(settings["purpose"]))

            try:
                if config.DEBUG:
                    result_prod_table_has_rows = 'true'
                else:
                    result_prod_table_has_rows = _RunSQLQueriesAndGetFirstQueryResult(
                        query_prod_table_has_rows, file_settings["database_name"])

                # String "true" because this query returns a boolean field "has_rows" that we're picking the value of
                if result_prod_table_has_rows == "true":
                    print(
                        "Test prod table has at least one row: SUCCESS_FULL. The table has at least one row.")
                    qc_result = config.qc_level.SUCCESS_FULL
                    qcRes = qcMessage('Test_Summary_Message',
                                      "Test prod table has at least one row: {qc_result}. The table has at least one row.".format(
                                          qc_result=qc_result.name), 'UserEmail', TestID)
                    qcResults.append(qcRes)
                    qcRes = qcMessage('Test_QC_Result', '',
                                      'UserEmail', TestID, qc_result)
                    qcResults.append(qcRes)
                    return qc_result
                print(
                    "Test prod table has at least one row: FAILURE_HARD. The table has no rows.")

                qc_result = config.qc_level.FAILURE_HARD
                qcRes = qcMessage('Test_Summary_Message',
                                  "Test prod table has at least one row: {qc_result}. The table has no rows.".format(
                                      qc_result=qc_result.name), 'UserEmail', TestID)
                qcResults.append(qcRes)
                qcRes = qcMessage('Test_QC_Result', '',
                                  'UserEmail', TestID, qc_result)
                qcResults.append(qcRes)
                return qc_result

            except RebuildProdTableException as e:
                qc_result = config.qc_level.FAILURE_HARD
                qcRes = qcMessage('Test_Messages',
                                  "Test prod table has at least one row: {qc_result}. Failed querying: {}".format(e, qc_result=qc_result.name), 'UserEmail', TestID)
                qcResults.append(qcRes)
                print(
                    "Test prod table has at least one row: FAILED. Failed querying: {}".format(e))
                return False  # Handle in exception

        # 6. SNIFF TEST: A FIELD MAY ONLY CONTAIN ALLOWED VALUES

        @register_qc_check("prod_field_contains_allowed_values")
        def _prod_field_contains_allowed_values(settings):
            TestID = hashlib.md5(pickle.dumps(settings)).hexdigest()
            print(
                "# 6. SNIFF TEST: A FIELD MAY ONLY CONTAIN ALLOWED VALUES")
            print("Test prod table field contains a set of allowed values: Begin")
            print("purpose: {}".format(settings["purpose"]))
            print("field_name: {}".format(settings["field_name"]))
            print("allowed_values: {}".format(settings["allowed_values"]))
            qcRes = qcMessage('Test_Messages',
                              "# 6. SNIFF TEST: A FIELD MAY ONLY CONTAIN ALLOWED VALUES", 'UserEmail', 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage(
                'Test_Messages', "Test prod table field contains a set of allowed values: Begin", 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage('Test_Purpose', "{}".format(
                settings["purpose"]), 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage(
                'Test_Messages', "field_name: {}".format(settings["field_name"]), 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage('Test_Messages', "allowed_values: {}".format(
                settings["allowed_values"]), 'UserEmail', TestID)
            qcResults.append(qcRes)

            values_in_data_table = _RunSQLQueriesAndGetQueryResultsFromColumn(
                "SELECT DISTINCT {field_name} FROM {database_name}.{table_name}".format(
                    field_name=settings["field_name"],
                    database_name=file_settings["database_name"],
                    table_name=prod_table_name
                ),
                file_settings["database_name"],
                settings["field_name"]
            )
            print("values_in_data_table (list): {}".format(values_in_data_table))
            qcRes = qcMessage(
                'Test_Messages', "values_in_data_table (list): {}".format(values_in_data_table), 'UserEmail', TestID)
            qcResults.append(qcRes)

            values_in_data_table = [x.strip().casefold()
                                    for x in values_in_data_table]
            values_in_allowed_values = [x.strip().casefold()
                                        for x in settings["allowed_values"]]

            invalid_values_in_data_table = list(
                set(values_in_data_table) - set(values_in_allowed_values))

            if len(invalid_values_in_data_table) > 0:
                qc_result = config.qc_level.FAILURE_HARD
                qcRes = qcMessage('Test_Summary_Message', "Test prod table field contains a set of allowed values: {qc_result}. The '{field_name}' field contains invalid values of {invalid_values_in_data_table}.".format(
                    qc_result=qc_result.name, field_name=settings["field_name"], invalid_values_in_data_table=invalid_values_in_data_table), 'UserEmail', TestID)
                qcResults.append(qcRes)
                qcRes = qcMessage('Test_QC_Result', '',
                                  'UserEmail', TestID, qc_result)
                qcResults.append(qcRes)
                print("Test prod table field contains a set of allowed values: FAILURE_HARD. The '{field_name}' field contains invalid values of {invalid_values_in_data_table}.".format(
                    field_name=settings["field_name"], invalid_values_in_data_table=invalid_values_in_data_table))
                return qc_result

            qc_result = config.qc_level.SUCCESS_FULL
            qcRes = qcMessage('Test_Summary_Message', "Test prod table field contains a set of allowed values: {qc_result}. The '{field_name}' field doesn't contain any invalid values.".format(
                qc_result=qc_result.name, field_name=settings["field_name"]), 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage('Test_QC_Result', '',
                              'UserEmail', TestID, qc_result)
            qcResults.append(qcRes)
            print("Test prod table field contains a set of allowed values: SUCCESS_FULL. The '{field_name}' field doesn't contain any invalid values.".format(
                field_name=settings["field_name"]))
            return qc_result

        # 7. SNIFF TEST: A FIELD MUST CONTAIN SPECIFIED VALUES

        @register_qc_check("prod_field_must_contain_these_values")
        def _prod_field_must_contain_these_values(settings):
            TestID = hashlib.md5(pickle.dumps(settings)).hexdigest()
            logger.debug(
                "# 7. SNIFF TEST: A FIELD MUST CONTAIN SPECIFIED VALUES")
            print(
                "# 7. SNIFF TEST: A FIELD MUST CONTAIN SPECIFIED VALUES")
            print("Test prod table field contains all specified values: Begin")
            print("purpose: {}".format(settings["purpose"]))
            print("field_name: {}".format(settings["field_name"]))
            print("required_values: {}".format(settings["required_values"]))
            qcRes = qcMessage('Test_Messages',
                              "# 7. SNIFF TEST: A FIELD MUST CONTAIN SPECIFIED VALUES", 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage(
                'Test_Messages', "Test prod table field contains all specified values: Begin", 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage('Test_Purpose', "{}".format(
                settings["purpose"]), 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage('Test_Messages', "field_name: {}".format(
                settings["field_name"]), 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage('Test_Messages', "required_values: {}".format(
                settings["required_values"]), 'UserEmail', TestID)
            qcResults.append(qcRes)

            values_in_data_table = _RunSQLQueriesAndGetQueryResultsFromColumn(
                "SELECT DISTINCT {field_name} FROM {database_name}.{table_name}".format(
                    field_name=settings["field_name"],
                    database_name=file_settings["database_name"],
                    table_name=prod_table_name
                ),
                file_settings["database_name"],
                settings["field_name"]
            )
            print("values_in_data_table (list): {}".format(values_in_data_table))
            qcRes = qcMessage('Test_Messages', "values_in_data_table (list): {}".format(
                values_in_data_table), 'UserEmail', TestID)
            qcResults.append(qcRes)

            values_in_data_table = [x.strip().casefold()
                                    for x in values_in_data_table]
            values_in_required_values = [x.strip().casefold()
                                         for x in settings["required_values"]]

            missing_required_values = list(
                set(values_in_data_table) - set(values_in_required_values))

            if len(missing_required_values) > 0:
                qc_result = config.qc_level.FAILURE_HARD
                qcRes = qcMessage('Test_Summary_Message', "Test prod table field contains certain required values: {qc_result}. The '{field_name}' field is missing these required values: {missing_required_values}.".format(
                    qc_result=qc_result.name, field_name=settings["field_name"], missing_required_values=missing_required_values), 'UserEmail', TestID)
                qcResults.append(qcRes)
                qcRes = qcMessage('Test_QC_Result', '',
                                  'UserEmail', TestID, qc_result)
                qcResults.append(qcRes)
                print("Test prod table field contains certain required values: FAILURE_HARD. The '{field_name}' field is missing these required values: {missing_required_values}.".format(
                    field_name=settings["field_name"], missing_required_values=missing_required_values))
                return qc_result

            qc_result = config.qc_level.SUCCESS_FULL
            qcRes = qcMessage('Test_Summary_Message', "Test prod table field contains certain required values: {qc_result}. The '{field_name}' field contains all required values.".format(
                qc_result=qc_result.name, field_name=settings["field_name"]), 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage('Test_QC_Result', '',
                              'UserEmail', TestID, qc_result)
            qcResults.append(qcRes)
            print("Test prod table field contains certain required values: SUCCESS_FULL. The '{field_name}' field contains all required values.".format(
                field_name=settings["field_name"]))
            return qc_result

        # 8. SNIFF TEST: THERE IS DATA FOR THE PREVIOUS MONTH

        @register_qc_check("prod_date_field_is_at_most_n_months_old")
        def _prod_date_field_is_at_most_n_months_old(settings):
            TestID = hashlib.md5(pickle.dumps(settings)).hexdigest()
            logger.debug(
                "# 8. SNIFF TEST: THERE IS DATA FOR THE PREVIOUS MONTH")
            print(
                "# 8. SNIFF TEST: THERE IS DATA FOR THE PREVIOUS MONTH")
            qcRes = qcMessage('Test_Messages',
                              "# 8. SNIFF TEST: THERE IS DATA FOR THE PREVIOUS MONTH", 'UserEmail', TestID)
            qcResults.append(qcRes)

            # Returns latest date from prod table (submitted data)
            date_field_string = _RunSQLQueriesAndGetFirstQueryResult(
                query_date_field_aggregation_query_template.format(
                    date_field_aggregation_function=settings["date_field_aggregation_function"],
                    date_field_name=settings["date_field_name"],
                    date_field_date_format_string=settings[
                        "date_field_date_format_string"] if "date_field_date_format_string" in settings else "",
                    database_name=file_settings["database_name"],
                    table_name=prod_table_name
                ),
                file_settings["database_name"]
            )

            date_field_object = datetime.strptime(
                date_field_string, "%Y-%m-%d").date()

            date_of_submission = date.today()

            # start with qc_level as FAILURE_HARD
            qc_result = config.qc_level(
                max([severity.value for severity in config.qc_level]))

            if date_field_object.month == date_of_submission.month:
                print("Test prod table date is the same month as the upload date: FAILED. The {date_field_aggregation_function} of '{date_field_name}' is {date_field_object}, which is the same month as the the submission date {date_not_later_than}".format(
                    date_field_aggregation_function=settings["date_field_aggregation_function"], date_field_name=settings["date_field_name"], date_field_object=date_field_object, date_not_later_than=date.today()))
                qcRes = qcMessage('Test_Summary_Message', "Test prod table date is the same month as the upload date: {qc_result}. The {date_field_aggregation_function} of '{date_field_name}' is {date_field_object}, which is the same month as the the submission date {date_not_later_than}".format(
                    qc_result=qc_result.name, date_field_aggregation_function=settings["date_field_aggregation_function"], date_field_name=settings["date_field_name"], date_field_object=date_field_object, date_not_later_than=date.today()), 'UserEmail', TestID)
                qcResults.append(qcRes)
                qcRes = qcMessage('Test_QC_Result', '',
                                  'UserEmail', TestID, qc_result)
                qcResults.append(qcRes)
                return qc_result

            # iterate through the (sorted) qc_levels and set qc_level_start to a lower value if conditions met
            # use enum values for order
            for severity in config.qc_level:
                if severity.name in settings['failure_threshold']:
                    months_between_data_date_and_submission_date = relativedelta(
                        date_of_submission, date_field_object).months
                    threshold_months_allowed = settings['failure_threshold'][
                        severity.name]['months_before_current_month']
                    if months_between_data_date_and_submission_date <= threshold_months_allowed:
                        qc_result = severity
                        break

            # fix the relativedelta section
            if qc_result == config.qc_level.SUCCESS_FULL or qc_result == config.qc_level.SUCCESS_WITH_NOTICE:
                print("Test prod table date is not later than date: {qc_level}. There is a {months_between_data_date_and_submission_date} month difference between the {date_field_aggregation_function} of '{date_field_name}', {date_field_object}, and the date of submission, {date_of_submission}, which is less than or equal to the threshold months allowed {threshold_months_allowed}".format(
                    qc_level=qc_result.name, date_field_aggregation_function=settings["date_field_aggregation_function"], date_field_name=settings["date_field_name"], date_field_object=date_field_object, months_between_data_date_and_submission_date=months_between_data_date_and_submission_date, date_of_submission=date_of_submission, threshold_months_allowed=threshold_months_allowed))
                qcRes = qcMessage('Test_Summary_Message', "Test prod table date is not later than date: {qc_result}. There is a {months_between_data_date_and_submission_date} month difference between the {date_field_aggregation_function} of '{date_field_name}', {date_field_object}, and the date of submission, {date_of_submission}, which is less than or equal to the threshold months allowed {threshold_months_allowed}".format(
                    qc_result=qc_result.name, date_field_aggregation_function=settings["date_field_aggregation_function"], date_field_name=settings["date_field_name"], date_field_object=date_field_object, months_between_data_date_and_submission_date=months_between_data_date_and_submission_date, date_of_submission=date_of_submission, threshold_months_allowed=threshold_months_allowed), 'UserEmail', TestID)
                qcResults.append(qcRes)
            else:
                qcRes = qcMessage('Test_Summary_Message', "Test prod table date is not later than date: {qc_result}. The {date_field_aggregation_function} of '{date_field_name}' is {date_field_object}, which is before the threshold date of {date_not_later_than}".format(
                    qc_result=qc_result.name, date_field_aggregation_function=settings["date_field_aggregation_function"], date_field_name=settings["date_field_name"], date_field_object=date_field_object, date_not_later_than=date_of_submission), 'UserEmail', TestID)
                qcResults.append(qcRes)
                print("Test prod table date is not later than date: {qc_level}. The {date_field_aggregation_function} of '{date_field_name}' is {date_field_object}, which is before the threshold date of {date_not_later_than}".format(
                    qc_level=qc_result.name, date_field_aggregation_function=settings["date_field_aggregation_function"], date_field_name=settings["date_field_name"], date_field_object=date_field_object, date_not_later_than=date_of_submission))

            qcRes = qcMessage(
                'Test_Messages', "Test prod table date contains last months data: Begin", 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage('Test_Purpose', "{}".format(
                settings["purpose"]), 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage('Test_Messages', "date_field_name: {}".format(
                settings["date_field_name"]), 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage('Test_Messages', "date_field_aggregation_function: {}".format(
                settings["date_field_aggregation_function"]), 'UserEmail', TestID)
            print("Test prod table date contains last months data: Begin")
            print("purpose: {}".format(settings["purpose"]))
            print("date_field_name: {}".format(settings["date_field_name"]))
            print("date_field_aggregation_function: {}".format(
                settings["date_field_aggregation_function"]))
            qcResults.append(qcRes)
            qcRes = qcMessage('Test_Messages', "date_field_date_format_string: {}".format(
                settings["date_field_date_format_string"] if "date_field_date_format_string" in settings else None), 'UserEmail', TestID)
            qcResults.append(qcRes)
            print("date_field_date_format_string: {}".format(
                settings["date_field_date_format_string"] if "date_field_date_format_string" in settings else None))

            qcRes = qcMessage('Test_Messages', "date_field (string): {}".format(
                date_field_string), 'UserEmail', TestID)
            qcResults.append(qcRes)

            qcRes = qcMessage('Test_Messages', "date_field (object): {}".format(
                date_field_object), 'UserEmail', TestID)
            qcResults.append(qcRes)

            qcRes = qcMessage('Test_QC_Result', '',
                              'UserEmail', TestID, qc_result)
            qcResults.append(qcRes)
            print("date_field (string): {}".format(date_field_string))

            print("date_field (object): {}".format(date_field_object))

            print(qc_result.name)
            return qc_result

        # 9. SNIFF TEST: A DATE FIELD IS NOT BEFORE A GIVEN DATE

        @register_qc_check("prod_date_field_is_equal_to_date")
        def _prod_date_field_is_equal_to_date(settings):
            TestID = hashlib.md5(pickle.dumps(settings)).hexdigest()
            logger.debug(
                "# 9. SNIFF TEST: A DATE FIELD IS NOT BEFORE A GIVEN DATE")
            print(
                "# 9. SNIFF TEST: A DATE FIELD IS NOT BEFORE A GIVEN DATE")
            print("Test prod table date is equal to a given date: Begin")
            print("purpose: {}".format(settings["purpose"]))
            print("date_field_name: {}".format(settings["date_field_name"]))
            print("date_field_aggregation_function: {}".format(
                settings["date_field_aggregation_function"]))
            print("date_field_date_format_string: {}".format(
                settings["date_field_date_format_string"] if "date_field_date_format_string" in settings else None))
            print("date_equal_to: {}".format(
                settings["date_equal_to"]))
            qcRes = qcMessage('Test_Messages',
                              "# 9. SNIFF TEST: A DATE FIELD IS NOT BEFORE A GIVEN DATE", 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage(
                'Test_Messages', "Test prod table date is equal to a given date: Begin", 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage('Test_Purpose', "{}".format(
                settings["purpose"]), 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage('Test_Messages', "date_field_name: {}".format(
                settings["date_field_name"]), 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage('Test_Messages', "date_field_aggregation_function: {}".format(
                settings["date_field_aggregation_function"]), 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage('Test_Messages', "date_field_date_format_string: {}".format(
                settings["date_field_date_format_string"] if "date_field_date_format_string" in settings else None), 'UserEmail', TestID)
            qcResults.append(qcRes)
            qcRes = qcMessage('Test_Messages', "date_equal_to: {}".format(
                settings["date_equal_to"]), 'UserEmail', TestID)
            qcResults.append(qcRes)

            if isinstance(settings["date_equal_to"], str):
                date_equal_to = datetime.strptime(
                    settings["date_equal_to"], "%Y-%m-%d").date()
                qcRes = qcMessage('Test_Messages', "date_equal_to (object): {}".format(
                    date_equal_to), 'UserEmail', TestID)
                qcResults.append(qcRes)
                print("date_equal_to (object): {}".format(
                    date_equal_to))
            else:
                raise RebuildProdTableException(
                    "Encountered an unknown 'date_equal_to' configuration")

            date_field_string = _RunSQLQueriesAndGetFirstQueryResult(
                query_date_field_aggregation_query_template.format(
                    date_field_aggregation_function=settings["date_field_aggregation_function"],
                    date_field_name=settings["date_field_name"],
                    date_field_date_format_string=settings[
                        "date_field_date_format_string"] if "date_field_date_format_string" in settings else "",
                    database_name=file_settings["database_name"],
                    table_name=prod_table_name
                ),
                file_settings["database_name"]
            )

            qcRes = qcMessage('Test_Messages', "date_field (string): {}".format(
                date_field_string), 'UserEmail', TestID)
            qcResults.append(qcRes)
            print("date_field (string): {}".format(date_field_string))

            date_field_object = datetime.strptime(
                date_field_string, "%Y-%m-%d").date()
            print("date_field (object): {}".format(date_field_object))
            qcRes = qcMessage('Test_Messages', "date_field (object): {}".format(
                date_field_object), 'UserEmail', TestID)
            qcResults.append(qcRes)

            if date_field_object == date_equal_to:
                qc_result = config.qc_level.SUCCESS_FULL
                qcRes = qcMessage('Test_Messages', "Test prod table date is equal to expected date, qc_check status is: {qc_result}. The {date_field_aggregation_function} of '{date_field_name}' is {date_field_object}, which equals the threshold date of {date_equal_to}".format(
                    qc_result=qc_result.name, date_field_aggregation_function=settings["date_field_aggregation_function"], date_field_name=settings["date_field_name"], date_field_object=date_field_object, date_equal_to=date_equal_to), 'UserEmail', TestID)
                qcResults.append(qcRes)
                print("Test prod table date is equal to expected date, qc_check status is: {severity_level}. The {date_field_aggregation_function} of '{date_field_name}' is {date_field_object}, which equals the threshold date of {date_equal_to}".format(
                    severity_level=qc_result.name, date_field_aggregation_function=settings["date_field_aggregation_function"], date_field_name=settings["date_field_name"], date_field_object=date_field_object, date_equal_to=date_equal_to))
            else:
                qc_result = config.qc_level.FAILURE_HARD
                print("Test prod table date is not equal to expected date, qc_check status is: {severity_level}. The {date_field_aggregation_function} of '{date_field_name}' is {date_field_object}, which equals the threshold date of {date_equal_to}".format(
                    severity_level=qc_result.name, date_field_aggregation_function=settings["date_field_aggregation_function"], date_field_name=settings["date_field_name"], date_field_object=date_field_object, date_equal_to=date_equal_to))
                qcRes = qcMessage('Test_Summary_Message', "Test prod table date is not equal to expected date, qc_check status is: {qc_result}. The {date_field_aggregation_function} of '{date_field_name}' is {date_field_object}, which equals the threshold date of {date_equal_to}".format(
                    qc_result=qc_result.name, date_field_aggregation_function=settings["date_field_aggregation_function"], date_field_name=settings["date_field_name"], date_field_object=date_field_object, date_equal_to=date_equal_to), 'UserEmail', TestID)
                qcResults.append(qcRes)
            qcRes = qcMessage('Test_QC_Result', '',
                              'UserEmail', TestID, qc_result)
            qcResults.append(qcRes)
            return qc_result

        print("")
        print("====================================")
        print("BEGIN DATA QUALITY CONTROL CHECKS")
        print("====================================")
        print("")

        qc_result = config.qc_level(
            min([severity.value for severity in config.qc_level]))  # set lowest qc_level value (SUCCESS_FULL) as starting point.
        # Ref. https://stackoverflow.com/a/54075852
        for check_settings in qc_settings:
            result = qc_check_functions[check_settings["type"]](check_settings)

            print("")
            print("============")
            print("")

            logger.debug("Result for qc: " +
                         check_settings['type'] + ":\n" + str(result))

            if(result.value > qc_result.value):
                # return max_severity_level back to functions.send_sns on line 115
                qc_result = result

        return qc_result

    # 0. Print some useful settings info
    for key, val in file_settings.items():
        if key not in ("create_prod_table_sql", "prod_table_qc_checks"):
            print("{}: {}".format(key, val))
    print("staging_table_name: {}".format(staging_table_name))
    print("prod_table_name: {}".format(prod_table_name))
    print("prod_snapshot_table_name: {}".format(prod_snapshot_table_name))

    # 1. SCAN THE STAGING TABLE FOR NEW PARTITIONS
    print("Scan staging for new partitions: Begin")
    # TODO: warning on new partitions not being found - if we change partitioning to by the hour or even minute (or even second) it means we should always have new partitions, right? Instead of partition_year/partition_month/etc., use partition_year=%Y%m%d-%H%M%S?
    try:
        if config.DEBUG:
            result_repair_staging_table = None
        else:
            result_repair_staging_table = _RunSQLQueriesAndGetFirstQueryResult(
                query_repair_staging_table, file_settings["database_name"])

        print("result_repair_staging_table")
        print(result_repair_staging_table)

        if result_repair_staging_table is not None:
            if result_repair_staging_table[0:6] == "Repair":
                print("Scan staging for new partitions: New partitions found")
                print(result_repair_staging_table)
            else:
                raise RebuildProdTableException(
                    "Got unknown response from repairing staging table: {}".format(result_repair_staging_table))
        else:
            print("Scan staging for new partitions: No new partitions found")

            # NOTE: No new partitions isn't a failure.
            # It could mean a file for a given day was overwritten with updated data, so we still need to continue and rebuild the prod table.
            # e.g. AIR data arrives at 13:45 (new partition) and 16:00 (no new partition, but data QA downstream can change the contents of the file)

    except RebuildProdTableException as e:
        raise RebuildProdTableException(
            "Failed scanning staging for new partitions: {}".format(e))

    # 2. SNIFF TEST: CAN WE STILL QUERY THE STAGING TABLE?
    print("Test querying the staging table: Begin")
    if _staging_table_has_rows():

        # 3. SNAPSHOT THE EXISTING PROD TABLE
        # snapshot_prod_{table_name}_[yyyymmdd]t[hhmm]
        print("Snapshotting prod table: Begin")
        if _snapshot_prod_table():

            # 4. DROP THE EXISTING PROD TABLE
            print("Drop prod table: Begin")
            if _drop_prod_table():

                # 5. REBUILD PROD TABLE
                print("Rebuild prod table: Begin")
                try:
                    _rebuild_prod_table()

                    # 6. CHECK THE PROD TABLES HAS ROWS
                    if _prod_has_rows():

                        # 7. SNIFF TEST: RUN SOME BASIC QC CHECKS AGAINST THE NEW PROD TABLE
                        print("Run QC checks against prod table: Begin")
                        qc_result = _runQCChecksOnProdTable(
                            file_settings["prod_table_qc_checks"])
                        if qc_result in (config.qc_level.SUCCESS_FULL, config.qc_level.SUCCESS_WITH_NOTICE):
                            print("Prod table rebuild completed successfully!")
                            return qc_result
                        else:
                            print("Prod table rebuild process failed!")
                            # 8. DROP THE BROKEN PROD TABLE AND ROLLBACK TO THE PROD SNAPSHOT
                            print("Rollback to prod snapshot: Begin")
                            _rollback_prod()
                            print(
                                "The rebuilt prod table failed QC checks, rolled back successfully. Check the latest data file to see what's up.")
                            return qc_result

                except RebuildProdTableException as e:
                    print(e)
                    logger.debug(e)
                    # 5.1 THE PROD TABLE DIDN'T REBUILD AND DOESN'T EXIST, SO WE ONLY NEED TO ROLLBACK TO THE PROD SNAPSHOT
                    print("Rollback to prod snapshot: Begin")
                    # TODO - need to handle different cases of prod tables existing and different places this exception is raised from in the qc and prod build process.
                    _restore_prod_from_snapshot()

                    # DQ - does RebuildProdTableException raise itself here?
                    if(len(e.args) > 1):
                        raise RebuildProdTableException(
                            "Failed rebuilding prod table, but we rolled back successfully. Check the prod table by-hand just to be sure.", e.args[1])
                    else:
                        qc_result = config.qc_level.FAILURE_HARD
                        raise RebuildProdTableException(
                            "Failed rebuilding prod table, but we rolled back successfully. Check the prod table by-hand just to be sure.", qc_result)

                except Exception as e:
                    print(e)
                    logger.debug(traceback.print_exc())
                    logger.debug(e)
                    qc_result = config.qc_level.FAILURE_HARD
                    raise RebuildProdTableException(
                        e, qc_result)
            else:
                # Clean snapshot manually
                # Deal with the repair breaking things manually
                raise RebuildProdTableException(
                    "Failed dropping the prod table (prior to rebuilding)")
        else:
            raise RebuildProdTableException(
                "Failed snapshotting the prod table (prior to rebuilding")
    else:
        raise RebuildProdTableException(
            "Failed scanning the staging table (prior to rebuilding")

# DQ - move these to sql function file?


def _RunSQLQueriesAndGetQueryResultAsIntegerOrNone(SQLString, DatabaseName):
    # Returns the value of the first row, or None if no rows were returned
    result = _RunSQLQueriesAndGetFirstQueryResult(SQLString, DatabaseName)
    return int(result) if result is not None else None


def _RunSQLQueriesAndGetFirstQueryResult(SQLString, DatabaseName):
    # Returns the value of the first row and column, or None if no rows were returned
    return _RunSQLQueries(
        SQLString,
        DatabaseName,
        {
            "mode": "FIRST_ROW_FIRST_COLUMN"
        }
    )


def _RunSQLQueriesAndGetQueryResultsFromColumn(SQLString, DatabaseName, ColumnName):
    # Returns the values of a given column as a list, or None if no rows were returned
    return _RunSQLQueries(
        SQLString,
        DatabaseName,
        {
            "mode": "COLUMN_VALUES",
            "ColumnName": ColumnName
        }
    )


def _RunSQLQueriesAndGetExecutionStatus(SQLString, DatabaseName):
    return _RunSQLQueries(SQLString, DatabaseName)


def _RunSQLQueries(SQLString, DatabaseName, GetValueFromSQLQueryResultsMode=None):
    # GetFirstValueFromSQLQueryResults: If true, returns value first cell in table, else returns execution status of query.
    logger.debug("Table SQL: {}".format(SQLString))

    # create client
    client = boto3.client('athena')

    logger.debug("start_query_execution: Begin")
    response = client.start_query_execution(
        # query variables to run in Athena
        QueryString=SQLString,
        QueryExecutionContext={
            'Database': DatabaseName
        },
        ResultConfiguration={
            'OutputLocation': config.ATHENA_QUERIES_OUTPUT_PATH,
        },
        WorkGroup=config.ATHENA_WORKGROUP_NAME
    )
    logger.debug("start_query_execution: End")

    execution_id = response['QueryExecutionId']
    logger.debug("_RunSQLQueries:\nexecution_id: "  # should just have the log return the current function
                 + execution_id)

    # Athena query execution states
    # QUEUED | RUNNING | SUCCEEDED | FAILED | CANCELLED
    # Ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/athena.html
    #
    # QUEUED: indicates that the query has been submitted to the service, and Athena will execute the query as soon as resources are available.
    # RUNNING: indicates that the query is in execution phase.
    # SUCCEEDED: indicates that the query completed without errors.
    # FAILED: indicates that the query experienced an error and did not complete processing.
    # CANCELLED: indicates that a user input interrupted query execution.
    #
    # Important Note: For CANCELLED, this is not strictly true.
    # Some classes of errors can cause a state of CANCELLED to be returned.
    # So far we've seen this for the "Bytes scanned limit was exceeded'" error.

    query_state = 'RUNNING'
    while (query_state in ['QUEUED', 'RUNNING']):
        response = client.get_query_execution(QueryExecutionId=execution_id)
        if 'QueryExecution' in response and 'Status' in response['QueryExecution'] and 'State' in response['QueryExecution']['Status']:
            query_state = response['QueryExecution']['Status']['State']
            logger.debug(
                "_RunSQLQueries:\n query_state: {}".format(query_state))

            if query_state == 'SUCCEEDED':
                # {
                #     'QueryExecution': {
                #         'QueryExecutionId': '1a72c85e-0bb6-467b-abd2-09c7f678d4b5',
                #         'Query': 'MSCK REPAIR TABLE department_of_test.staging_dot_my_test_dataset',
                #         'StatementType': 'DDL',
                #         'ResultConfiguration': {
                #             'OutputLocation': 's3://datainsightsplatform-prod-athena-queries/uc1/1a72c85e-0bb6-467b-abd2-09c7f678d4b5.txt'
                #         },
                #         'QueryExecutionContext': {
                #             'Database': 'department_of_test'
                #         },
                #         'Status': {
                #             'State': 'SUCCEEDED',
                #             'SubmissionDateTime': datetime.datetime(2021, 9, 8, 5, 48, 48, 4000, tzinfo=tzlocal()), 'CompletionDateTime': datetime.datetime(2021, 9, 8, 5, 48, 56, 713000, tzinfo=tzlocal())
                #         },
                #         'Statistics': {'EngineExecutionTimeInMillis': 8473, 'DataScannedInBytes': 0, 'TotalExecutionTimeInMillis': 8709, 'QueryQueueTimeInMillis': 210, 'ServiceProcessingTimeInMillis': 26},
                #         'WorkGroup': 'department-of-communities',
                #         'EngineVersion': {'SelectedEngineVersion': 'AUTO', 'EffectiveEngineVersion': 'Athena engine version 2'}
                #     },
                #     'ResponseMetadata': {
                #         'RequestId': 'f57290c3-6335-4eb0-affd-c5f21647b517', 'HTTPStatusCode': 200, 'HTTPHeaders': {'content-type': 'application/x-amz-json-1.1', 'date': 'Wed, 08 Sep 2021 05:48:57 GMT', 'x-amzn-requestid': 'f57290c3-6335-4eb0-affd-c5f21647b517', 'content-length': '1521', 'connection': 'keep-alive'}, 'RetryAttempts': 0
                #     }
                # }

                query_result = query_state
                if GetValueFromSQLQueryResultsMode is not None:
                    if GetValueFromSQLQueryResultsMode["mode"] == "FIRST_ROW_FIRST_COLUMN":
                        # Returns None if no results were returned
                        query_result = client.get_query_results(
                            QueryExecutionId=execution_id)
                        query_result = _ReturnFirstResultFromSQLQuery(
                            query_result)
                    # TODO - convert this to better handle the count of rows vs 'are there rows' and check athena metadata response to see if this information is already returned:
                    #   query_result = if query_result = 'true':
                    #       True
                    #   else query_result
                    if GetValueFromSQLQueryResultsMode["mode"] == "COLUMN_VALUES":
                        # Returns None if no results were returned
                        query_result = client.get_query_results(
                            QueryExecutionId=execution_id)
                        query_result = _ReturnResultColumnFromSQLQuery(
                            query_result, GetValueFromSQLQueryResultsMode["ColumnName"])
                return query_result
            # TESTTHIS - query states of failed and cancelled
            elif query_state == 'FAILED':
                if "StateChangeReason" in response['QueryExecution']['Status']:
                    query_result = "{} - {}".format(
                        query_state, response['QueryExecution']['Status']['StateChangeReason'])
                else:
                    query_result = query_state

                raise RebuildProdTableException(query_result)
                # return result

            elif query_state == 'CANCELLED':
                # {
                #     'QueryExecution': {
                #         'QueryExecutionId': '770ba126-5a94-4eed-8cc9-f347216e8b87',
                #         'Query': 'SELECT COUNT(*) > 0 AS has_rows FROM department_of_test.staging_dot_my_test_dataset',
                #         'StatementType': 'DML',
                #         'ResultConfiguration': {
                #             'OutputLocation': 's3://datainsightsplatform-prod-athena-queries/uc1/770ba126-5a94-4eed-8cc9-f347216e8b87.csv'
                #         },
                #         'QueryExecutionContext': {
                #             'Database': 'department_of_test'
                #         },
                #         'Status': {
                #             'State': 'CANCELLED',
                #             'StateChangeReason': 'Bytes scanned limit was exceeded',
                #             'SubmissionDateTime': datetime.datetime(2021, 9, 8, 5, 40, 26, 988000, tzinfo=tzlocal()), 'CompletionDateTime': datetime.datetime(2021, 9, 8, 5, 40, 30, 353000, tzinfo=tzlocal())
                #         },
                #         'Statistics': {'EngineExecutionTimeInMillis': 3192, 'DataScannedInBytes': 52428800, 'TotalExecutionTimeInMillis': 3365, 'QueryQueueTimeInMillis': 121, 'QueryPlanningTimeInMillis': 493, 'ServiceProcessingTimeInMillis': 52},
                #         'WorkGroup': 'department-of-communities',
                #         'EngineVersion': {
                #             'SelectedEngineVersion': 'AUTO',
                #             'EffectiveEngineVersion': 'Athena engine version 2'
                #         }
                #     },
                #     'ResponseMetadata': {
                #         'RequestId': 'bb2ed6e0-edf3-47eb-b988-37884ae86f08',
                #         'HTTPStatusCode': 200,
                #         'HTTPHeaders': {
                #             'content-type': 'application/x-amz-json-1.1',
                #             'date': 'Wed, 08 Sep 2021 05:40:30 GMT',
                #             'x-amzn-requestid': 'bb2ed6e0-edf3-47eb-b988-37884ae86f08',
                #             'content-length': '1730',
                #             'connection': 'keep-alive'
                #         },
                #         'RetryAttempts': 0
                #     }
                # }

                query_result = "{} - {}".format(
                    query_state, response['QueryExecution']['Status']['StateChangeReason'])
                raise RebuildProdTableException(query_result)
                # return result

        time.sleep(1)


def _ReturnFirstResultFromSQLQuery(result):
    # "> 1" because there's a header row and then one or more rows of values (think CSV file)
    if len(result['ResultSet']['Rows']) > 1:
        logger.debug("_ReturnFirstResultFromSQLQuery:\n"
                     + json.dumps(result['ResultSet']))

        # Example data structure
        # {
        #     'Rows': [
        #         {
        #             'Data': [{'VarCharValue': 'has_rows'}]
        #         },
        #         {
        #             'Data': [{'VarCharValue': 'true'}]
        #         }
        #     ],
        #     'ResultSetMetadata': {
        #         'ColumnInfo': [{
        #             'CatalogName': 'hive',
        #             'SchemaName': '',
        #             'TableName': '',
        #             'Name': 'has_rows',
        #             'Label': 'has_rows',
        #             'Type': 'boolean',
        #             'Precision': 0,
        #             'Scale': 0,
        #             'Nullable': 'UNKNOWN',
        #             'CaseSensitive': False
        #         }]
        #     }
        # }

        result = result['ResultSet']['Rows'][1]['Data'][0]['VarCharValue']
        return result
    else:
        return None


def _ReturnResultColumnFromSQLQuery(result, ColumnName):
    # "> 1" because there's a header row and then one or more rows of values (think CSV file)
    if len(result['ResultSet']['Rows']) > 1:
        logger.debug("_ReturnResultColumnFromSQLQuery:\n"
                     + json.dumps(result['ResultSet']))

        # Example data structure
        # {
        #     "Rows": [{
        #             "Data": [{
        #                 "VarCharValue": "aboriginal"
        #             }]
        #         },
        #         {
        #             "Data": [{
        #                 "VarCharValue": ""
        #             }]
        #         },
        #         {
        #             "Data": [{
        #                 "VarCharValue": "N"
        #             }]
        #         },
        #         {
        #             "Data": [{
        #                 "VarCharValue": "Y"
        #             }]
        #         }
        #     ],
        #     "ResultSetMetadata": {
        #         "ColumnInfo": [{
        #             "CatalogName": "hive",
        #             "SchemaName": "",
        #             "TableName": "",
        #             "Name": "aboriginal",
        #             "Label": "aboriginal",
        #             "Type": "varchar",
        #             "Precision": 2147483647,
        #             "Scale": 0,
        #             "Nullable": "UNKNOWN",
        #             "CaseSensitive": True
        #         }]
        #     }
        # }

        columnIndexes = [i for i, d in enumerate(
            result['ResultSet']['ResultSetMetadata']['ColumnInfo']) if d["Name"] == ColumnName]
        if len(columnIndexes) != 1:
            raise RebuildProdTableException("Found {} column values while searching for ColumnName '{}'. This is a problem. Result: {}".format(
                len(columnIndexes), ColumnName, columnIndexes))
        columnIndex = columnIndexes[0]

        # [1:] to skip the first row (headers)
        # Assumes all results are varchar strings
        result = [row["Data"][columnIndex]["VarCharValue"]
                  for row in result['ResultSet']['Rows'][1:]]
        return result
    else:
        return None


# for running locally, uses json file for event input
if __name__ == "__main__" and config.ENVIRONMENT_LOCAL:
    testevent = Path(__file__).with_name(
        'testevent.json')
    with open(testevent, 'r') as f:
        eventstring = json.load(f)

    lambda_handler(eventstring, "")
