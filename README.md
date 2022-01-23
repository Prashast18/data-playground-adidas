# data-stage-adidas
Repository for Case study of Adidas



Technologies
	Data Storage:
•	S3
•	MPP
•	Data Integration with Existing sources
Data Processing:
•	Apache Spark
Data Warehouse/Data Pipeline:
•	Hive 
Data Exploitation/Reporting:
•	Zeppelin (Spark SQL)
•	Hive

Problems with Existing Infrastructure
Exasol - Alteryx
1.	When the volume of your data is significant Alteryx would be quite slow since Alteryx doesn’t support distributed data processing (no MapReduce or PySpark support).
2.	Alteryx stores temporary data on disk, it doesn’t support distributed data storage like S3, Azure Blob Storage, etc.
3.	Alteryx lacks good visualization capabilities.
4.	Alteryx doesn’t support any operating systems other than Windows. You cannot use Alteryx on Mac or Linux.
5.	Vendor lock-in: Alteryx is using a proprietary format for data workflows and you cannot export an Alteryx pipeline and run it in an open-source environment.
6.	Alteryx can give the outputs of Tableau data extract but it is not capable of reading a similar extract.
7.	Alteryx isn’t open source. You cannot run Alteryx pipelines without a paid license.
8.	Scrolling within the workflows
9.	If you want to trigger Alteryx pipeline runs automatically you would have to pay for an expensive Alteryx server license.
Big Data Platform – AWS
1.	Lack of Version Control System
2.	Lack of CI/CD for code deployment
3.	ETL Pipeline to developed/scheduled/monitored.

Implementation details
1.	Migrate data pipelines to AWS:
•	Glue ETL jobs to rescue. Use Cloud Event Rules for event/time-based trigger & Step functions for orchestration. In case of less complicated workflows, leverage Glue ETL jobs trigger for Scheduling.
•	Migrate pipelines involving complex transformations to Spark- Scala/Python and orchestrate using Airflow. These jobs will be submitted to the EMR cluster using an EMR step/Livy
•	Segregate the workflows based on the use-case & access pattern. An application that involves listening to a REST API or sending the data to an API endpoint fit for this implementation. Leverage Docker containers & AWS EKS for the same.
2.	Integrate Zeppelin/Jupyter Notebooks with GIT for code sync & version control.
3.	After the development is complete, 
•	Dev CI/CD-Test-CI/CDProduction
4.	Schedule Pipeline using Apache Airflow (AWSMA)
5.	Monitor the pipeline using Ganglia/CloudWatch Events/AWS CloudTrail or integrate the tools with Prometheus & Grafana.

Design Criterion
1.	Scalability - 
2.	Fault Tolerance/Retry
3.	Scheduling & Monitoring - 
4.	Handling inconsistencies in data 
a.	Assuming that the data received is never right, I would suggest some basic cleansing operations to be done on the data. 
b.	Once we are done with the scrubbing of data, we need to have schema check on the data to ensure correct datatypes. We can start with plugging in data schema in every data pipeline and then move to a central data registry for making this more flexible & robust
5.	Data Modelling 
a.	Effective use of partitions, and file formats for persisting the data. I would suggest to go ahead with Parquet files since the data in S3 needs to be accessed via Spark Jobs.
b.	Redshift – Choose right sort key, distribution key, and use load commands for leveraging parallel processing capabilities of Redshift.

Handling Data Merge
<>

Testing Pipeline
<>


Scheduling & Monitoring:

1- Jupyter/Zeppelin Notebooks
Can be scheduled using Cron from the Zeppelin UI or POST request. However, this is only advisable for the development or testing environments	. In a production scale environment, we should be using either 2 or 3.

2 – Glue PySpark 
a)	Use Step functions to orchestrate the workflows
b)	For monitoring, use the inbuilt Glue Monitoring UI. CloudWatch logs & CloudTrail to monitor the API requests made to S3.

3 - Spark jobs submitted to EMR
a)	To be scheduled using Airflow (AWS Managed Airflow)
b)	Can be monitored using Ganglia/CloudWatch Events/ AWS Airflow UI. The workloads on EMR can be monitored and optimised using Prometheus & Grafana.
	
