The repo. consists of basic scripts which convert csv files to parq and vice versa. I have used fastparquet python api for this .
There is also a script which uses Spark to create the Parquet files and then query it using SparkSql.
The roles folder contains tasks folder with a main.yml. This is a Ansible playbook whcih installs pandas, fastparquet on a spark cluster you have created. 
It downloads the adult.data from UCI website and then creates a parquet file adult.parquet and then uses SparkSql to query the parquet file.
