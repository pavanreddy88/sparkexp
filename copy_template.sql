TRUNCATE TABLE <schema>.<table>
;

COPY <schema>.<table>
FROM 's3://rda-rdm-uw2-scratch-prd/datasets/<table>/prod/'
CREDENTIALS 'aws_iam_role=arn:aws:iam::823951367757:role/redshift_s3_role'
FORMAT AS PARQUET
;