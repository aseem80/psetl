# Product Service ETL #

This repo contains source code for ETL jobs for product service

## Getting Started ##

This project gets built with maven. The technology stack used is Spark, Spark Cassandra Connector, Spring Boot, Cassandra

## Prerequisites ##

Java 8, Maven, Spark Set up, Cassndra Connection info and define PS_ETL_TASKS_JAR

## Overall Architecture ##

![Diagram](/projects/PSLL/repos/psetl/browse/src/main/documents/PSETLDesign.jpg)

## Program Build Output ##

Project is built with Maven and creates two Excecutables jar.
1. product-data-tasks.jar :- This jar is produced under path set up in environment varaiable PS_ETL_TASKS_JAR.
   This jar gets distributed to spark cluster to execute distributed tasks
2. ps-etl-${VERSION}.jar :-  This jar is Spark Driver program embedded in Spring Boot app. This has been configured to
   run every midnight(12 AM) by default and can be overriden by environment variable()PS_CRONTAB_EXPRESSION  and
   creates file in S3 buckets as defined in environment varaible S3_LOCATION


## Access Control and integration with S3 ##

1. The s3 protocol is supported in Hadoop, but does not work with Apache Spark unless you are using the AWS version of
Spark in Elastic MapReduce (EMR). We can safely ignore this protocol for now.
2. The s3n protocol is Hadoop's older protocol for connecting to S3. Implemented with a third-party library (JetS3t),
it provides rudimentary support for files up to 5 GB in size and uses AWS secret API keys to run.
This "shared secret" approach is brittle, and no longer the preferred best practice within AWS.
 It also conflates the concepts of users and roles, as the worker node communicating with S3 presents itself as
 the person tied to the access keys.
3. The s3a protocol is successor to s3n but is not mature yet. Implemented directly on top of AWS APIs, it is faster,
handles files up to 5 TB in size, and supports authentication with Identity and Access Management (IAM) Roles.
With IAM Roles, you assign an IAM Role to your driver/worker nodes and then attach policies granting access to your S3
bucket. No secret keys are involved, and the risk of accidentally disseminating keys or committing them in version
control is reduced. s3a support was introduced in Hadoop 2.6.0, but several important issues were corrected in version
2.7.0 (with more on the horizon in 2.8.0).

4. The dependency versions(org.apache.hadoop:hadoop-aws:2.6.0 and com.amazonaws:aws-java-sdk:1.7.4) used in this project
 is compatible with Spark distribution(version 1.6.3) deployed in SIT/production environments


