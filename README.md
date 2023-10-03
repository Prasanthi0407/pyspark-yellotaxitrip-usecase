# Yellow Taxi Data 2020 - Jan: ETL and Analysis

## Project Description: 

This task is to perform a simple data processing and deliver by using Yellow Taxi Trip dataset from https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page for 2020 January Yellow Taxi Trip Records (parquet). More information is available in the "Data Dictionaries and MetaData" section.

First stage of the ETL processing is to partition the output files by drop off date and then the second step is to load all partitioned files into mysql database for further analysis.

### Softwares 

Pyspark 3.5
Spark 3.0.3
Hadoop 2.7
Jdbc Mysql 8.0.33

#### How to install and run the project code: 

 Local Execution


1.	Mentioned the software versions above, ensure to have the same versions available to avoid any version dependency issues.
2.	To support store the output data in mysql, have to place the installed mysql jar in to spark installed location (ex: homeware\spark-3.0.3-bin-hadoop2.7\jars).
3.	Setup PYTHONPATH in environment variables (ex: %SPARK_HOME%\python;%SPARK_HOME%\python\lib\py4j-<specific version>-src.zip;%PYTHONPATH%) 


##### Test cases

Sample Py test created to check the column counts and row counts from expected and actual transformed data frame.
