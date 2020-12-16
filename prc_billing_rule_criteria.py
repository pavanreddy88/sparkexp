#!/usr/bin/python
from pyspark.sql import SparkSession
import datetime
import subprocess
from dateutil import tz


def main():

    # ############## assign standard variables ############## #

    v_table = "billing_rule_criteria"

    v_session = SparkSession.builder.appName("prc_" + v_table).getOrCreate()

    v_target_bucket = "rda-rdm-uw2-processing-prd/analytics/" + v_table

    v_source_bucket = "rda-rdm-uw2-stage-prd/payments/datasets"

    v_today = datetime.datetime.now().replace(tzinfo=tz.gettz("UTC")).astimezone(tz.gettz("US/Pacific")) \
        .strftime("%Y%m%d%H%M%S")

    v_cp_source = "s3://" + v_target_bucket + "/prod/"

    v_cp_target = "s3://" + v_target_bucket + "/bkp/" + v_today + "/"

    v_partition_count = 1

    v_success_file = subprocess.check_output(
        "aws s3 ls " + v_cp_source + " --recursive | grep _SUCCESS | wc -l | cat", shell=True
    )

    # ############## backup prod data set ############## #

    if int(v_success_file) >= 1:

        subprocess.check_output("aws s3 rm " + v_cp_target + " --recursive | cat", shell=True)

        subprocess.check_output("aws s3 sync " + v_cp_source + " " + v_cp_target + " | cat", shell=True)

    # ############## loop source tables, isolate most recent data set, and create temp tables ############## #

    l_tables = [
        ["pmtmart", "prc_owner", "billing_rule_criteria"]

    ]

    for row in l_tables:

        v_output = subprocess.check_output(
            "aws s3 ls s3://" + v_source_bucket + "/" + row[0] + ".db/" + row[1] + "/" + row[2]
            + "/ | grep =20 | awk \'NF{ print $NF }\' | grep / | tail -1", shell=True
        )

        v_folder = v_output[:-1]

        v_session.read.parquet(
            "s3://" + v_source_bucket + "/" + row[0] + ".db/" + row[1] + "/" + row[2] + "/" + v_folder + "*"
        ).createOrReplaceTempView(row[0] + "_db__" + row[1] + "__" + row[2])

        # ############## create prod data frame ############## #

    hd_prod = """
        SELECT      INT(ruleid) AS rule_id,
                    pricing_category,
                    transaction_type,
                    rulename AS rule_name,
                    rule_description,
                    fee_type,
                    INT(rate_unit) AS rate_unit,
                    INT(fee_category) AS fee_category,
                    INT(mass_bucket) AS mass_bucket,
                    INT(gl_account) AS gl_account,
                    BOOLEAN(ruleversion) AS rule_version,
                    BOOLEAN(ruleactive) AS rule_active,
                    transaction_reason,
                    CAST(rate AS DECIMAL(38,9)) AS rate,
                    --FROM_UTC_TIMESTAMP(last_modified, 'America/Los_Angeles') AS last_modified
                    last_modified
        FROM        pmtmart_db__prc_owner__billing_rule_criteria
"""

    df_prod = v_session.sql(hd_prod)

    df_prod.repartition(v_partition_count).write.mode("overwrite").parquet("s3://" + v_target_bucket + "/prod")


if __name__ == "__main__":
    main()
