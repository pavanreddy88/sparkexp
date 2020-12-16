#!/usr/bin/python
from pyspark.sql import SparkSession
import datetime
from dateutil import tz
import subprocess
import argparse


def main():

	# ############## set up parameter(s) ############## #

	parser = argparse.ArgumentParser()

	parser.add_argument(
		"--debug_mode",
		help="debug mode will save the temp directory in S3",
		nargs="?",
		type=int,
		default=0
	)

	args = parser.parse_args()

	# ############## assign standard variables ############## #

	v_debug_mode = args.debug_mode

	# ############## assign standard variables ############## #

	v_table = "<destination_table_name>"

	v_session = SparkSession.builder.appName("prc_" + v_table).getOrCreate()

	v_target_bucket = "rda-rdm-uw2-scratch-prd/datasets/" + v_table

	v_source_bucket = "rda-rdm-uw2-stage-prd/payments/datasets"

	v_today = datetime.datetime.now().replace(tzinfo=tz.gettz("UTC")).astimezone(tz.gettz("US/Pacific"))\
		.strftime("%Y%m%d%H%M%S")

	v_sq_root = str(int(int(v_today) * 1000000000000000))

	v_id_root = str(int(int(v_today) * 1000000000000000000000000))

	v_cp_source = "s3://" + v_target_bucket + "/prod/"

	v_cp_target = "s3://" + v_target_bucket + "/bkp/" + v_today + "/"

	v_partition_count = 100

	v_success_file = subprocess.check_output(
		"aws s3 ls " + v_cp_source + " --recursive | grep _SUCCESS | wc -l | cat", shell=True
	)

	# ############## backup prod data set ############## #

	if int(v_success_file) >= 1:

		subprocess.check_output("aws s3 rm " + v_cp_target + " --recursive | cat", shell=True)

		subprocess.check_output("aws s3 sync " + v_cp_source + " " + v_cp_target + " | cat", shell=True)

		subprocess.check_output("emrfs sync s3://" + v_cp_target + " | cat", shell=True)

	# ############## loop source tables, isolate most recent data set, and create temp tables ############## #

	l_tables = [
		["<source_data_lake_database>", "<source_table_name>"]
	]

	for row in l_tables:

		v_output = subprocess.check_output(
			"aws s3 ls s3://" + v_source_bucket + "/" + row[0] + ".db/" + row[1]
			+ "/ | grep =20 | awk \'NF{ print $NF }\' | grep / | tail -1", shell=True
		)

		v_folder = v_output[:-1]

		v_session.read.parquet("s3://" + v_source_bucket + "/" + row[0] + ".db/" + row[1] + "/" + v_folder + "*")\
			.createOrReplaceTempView(row[0] + "_db__" + row[1])

		v_session.catalog.cacheTable(row[0] + "_db__" + row[1])

	# ############## create prod data frame ############## #

	hd_prod = """
		select		MONOTONICALLY_INCREASING_ID() AS id,																	-- creates locally unique, increasing, non-consecutive integers
					ROW_NUMBER() OVER (ORDER BY colA) AS id,																-- creates locally unique, increasing, consecutive integers
					CAST('{v_id_root}' AS DECIMAL(38,0)) + CAST(MONOTONICALLY_INCREASING_ID() AS DECIMAL(38,0)) AS rec_id,	-- creates globally unique, increasing, non-consecutive integers
					BIGINT('{v_sq_root}') + ROW_NUMBER() OVER(ORDER BY colA) AS id,											-- creates globally unique, increasing, consecutive integers
					BIGINT(colA) AS colA, 																					-- convert decimal column to bigint
					STRING(BIGINT(ColB)) AS colB, 																			-- convert decimal column with whole numbers to string
					DATE(FROM_UTC_TIMESTAMP(colC, 'America/Los_Angeles')) AS colC, 															-- convert timestamp column to date and PST
					FROM_UTC_TIMESTAMP(colD, 'America/Los_Angeles') AS colD, 																-- convert timestamp column to PST
					STRING(NULL) AS colE, 																					-- convert NULL to destination datatype
					CAST(colF AS DECIMAL(19,4)) AS colF, 																	-- convert decimal column to money precision and scale
					colG, 																									-- STRING columns do not need conversions
					INT(colH) AS colH, 																						-- convert decimal to integer
					BOOLEAN(colI) AS colI, 																					-- convert decimal to boolean,
					DOUBLE(colJ) AS colJ 																					-- convert decimal to float
		FROM		<source_data_lake_database>_db__<source_table_name>
	"""

	df_prod = v_session.sql(hd_prod.replace("{v_id_root}", v_id_root).replace("{v_sq_root}", v_sq_root))

	df_prod.repartition(v_partition_count).write.mode("overwrite").parquet("s3://" + v_target_bucket + "/prod")

	# ############## uncache tables ############## #

	v_session.catalog.clearCache()

	# ############## remove temp data sets ############## #

	if v_debug_mode == 0:

		subprocess.check_output("aws s3 rm s3://" + v_target_bucket + "/temp/ --recursive | cat", shell=True)

		subprocess.check_output("emrfs sync s3://" + v_target_bucket + "/temp/ | cat", shell=True)


if __name__ == "__main__":
	main()
