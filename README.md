# Welcome to Change Discovery Capture from SQL and NoSQL to a Data Warehouse!

Suppose you are ingesting large amounts of data into SQL and NoSQL. You got big data! For Data Engineering, there are the 3 Vs: volume, velocity, variety. Both SQL and NoSQL can take care of volume and velocity if they are transactional databases. However NoSQL can take care of variety with semi-structured and unstrucuted data. However you also want OLAP data warehouse for easy querying for business analytics. To replicate the source databases to the target data warehouse is a process called Change Discovery Capture (CDC).



# Architecture
<p align="center"><img src="arch_diagram.jpg" width="800"></p>
The architecture diagram looks quick intense. The core idea is quite simple: there are 3 databases: SQL (RDS with MySQL), NoSQL (DynamoDB), and data warehouse (Redshift). Here are the moving parts:

* Every 5 minutes, Eventbridge triggers a Lambda to load `txns.csv` to RDS. Since I defined the table with no primary key/uniqueness restriction, the table gets appended. AWS DMS (data migration service) task is synchronize the data from RDS to Redshift via CDC.
* Every 5 minutes, Eventbridge triggers a Lambda to load `trades.json` to DynamoDB. Any INSERTS or UPDATES triggers DynamoDB stream to trigger another separate Lambda that will write those new records into a file stored in an S3 bucket. Every 5 minutes, another Lambda will load files from the S3 bucket to the Redshift cluster, then delete the files.

For observability, you can inspect the Lambda's Cloudwatch logs: runtime duration, failures, and count of endpoint hits. If you are fancy, you can add metrics & alarms to the Lambda (and API Gateway). For the business/operations/SRE team, you can add New Relic to the Lambda such that there will be "single pane of glass" for 24/7 monitoring. You can also inspect the API Gateway's dashboard.



## Miscellaneous details:
* `cdk.json` is basically the config file. I specified to deploy this microservice to us-east-1 (Virginia). You can change this to your region of choice.
* As always, IAM permissions and VPC/security groups are the trickiest parts.
* The following is the AWS resources deployed by CDK and thus Cloudformation. A summary would be: <p align="center"><img src="AWS_resources.jpg" width="500"></p>
    * 1 RDS instance
    * 1 DynamoDB Table
    * 1 Redshift cluster
    * 5 Lambda functions
    * 1 DMS instance
    * 1 DMS replication task
    * 1 S3 bucket
    * other miscellaneous AWS resources
* Redshift table should match **RDS** table exactly within seconds due to DMS migration task. However Redshift table will not match **DynamoDB** table exactly in the case that you delete records from DynamoDB table; determine what to do with deleted DynamoDB records if they need to also deleted from Redshift table.
* Useful (dynamically-created) details are displayed in Cloudformation Outputs: Redshift endpoint, RDS endpoint, DynamoDB table name, S3 bucket name.
* If you delete this Cloudformation stack, then it will delete all the AWS resources including stateful resources such as RDS instance, DynamoDB table, Redshft cluster, S3 bucket. You can change the `removal_policy` of the AWS resources if you want them retained instead of deleted.
* If you delete this stack, first manually stop the DMS migration task; otherwise the stack will not fully delete, ie some AWS resources will remain undeleted.



# TODOs to Meet Production Requirements
* Write unit tests for the Lambda code
* Disable RDS's publicly accessible endpoint if not needed
* Instead of hard coding the login credentials for RDS and Redshift in `cdk.json`, use Secrets Manager instead
* Tighten IAM permissions/roles on AWS resources to follow Principle of Least Privilege
* Tighten the VPC's security groups such that Inbound Rules only allow connections from within the VPC and/or whitelisted IP addresses
* Create VPC instead of using default VPC if necessary to enforce stronger rules or need the flexibility
* Add AWS Quicksight to Redshift for business analytics



# Deploying the Microservice Yourself
```
$ python -m venv .venv
$ source .venv/bin/activate
$ python -m pip install -r requirements.txt
$ cdk deploy  # Docker daemon must be running; also assumes AWS CLI is configured + npm installed with `aws-cdk`: detailed instructions at https://cdkworkshop.com/15-prerequisites.html
```