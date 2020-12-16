#!/usr/bin/python
from pyspark.sql import SparkSession
import subprocess


def main():

	# ############## assign standard variables ############## #

	v_table = "zztmp_all_apps"

	v_session = SparkSession.builder.appName("cnv_" + v_table).getOrCreate()

	v_target_bucket = "rda-rdm-uw2-processing-prd/analytics/" + v_table

	v_source_bucket = "rda-rdm-uw2-dms-migration-prd/Analytics/parquet"

	v_partition_count = 25

	# ############## loop source tables, isolate most recent data set, and create temp tables ############## #

	l_tables = [
		["dbo", "zztmp_all_apps"]
	]

	for row in l_tables:

		v_output = subprocess.check_output(
			"aws s3 ls s3://" + v_source_bucket + "/" + row[0] + ".db/" + row[1]
			+ "/ | grep =20 | awk \'NF{ print $NF }\' | grep / | tail -1", shell=True
		)

		v_folder = v_output[:-1]

		v_session.read.parquet("s3://" + v_source_bucket + "/" + row[0] + "/" + row[1] + "/" + v_folder + "*")\
			.createOrReplaceTempView(row[0] + "_db__" + row[1])

	# ############## create prod data frame ############## #

	hd_prod = """
		select
				MID_UWDID,
				MerchantId,
				Zoot_App_Date,
				CQ_YN,
				CQ_Result,
				ApplicationChannel,
				ApplicationSource,
				Auto_Decision,
				DimIMSStatus,
				Last_DimStatus_Date,
				UW_Pended_FLOW,
				UW_Pended_Group,
				UW_Flow_Queue_Ct,
				UW_Rules_Flagged,
				UW_Rules_Flagged_Ct,
				Rule_Category,
				int(CreditScore) as CreditScore,
				CreditScore_Grp,
				DroolsDecision,
				ComplianceDecision,
				FraudDecision,
				CreditDecision
		from dbo_db__zztmp_all_apps
	"""

	df_prod = v_session.sql(hd_prod)

	df_prod.repartition(v_partition_count).write.mode("overwrite").parquet("s3://" + v_target_bucket + "/prod")


if __name__ == "__main__":
	main()
