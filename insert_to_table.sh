#!/bin/bash
cd /Users/kholedelacruz/Documents/git/big-data-beams/processedData
for f in *.csv
do
	mysql -e "load data local infile '"$f"' into table taxi_trip fields TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES"  -u khole --password=12345678 --host=taxi-trip-db.ckm7uxlcgbsp.us-east-1.rds.amazonaws.com taxi
done
