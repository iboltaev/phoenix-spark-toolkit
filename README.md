# phoenix-spark-toolkit
toolkit for working with Apache Phoenix + Apache Spark

Apache Phoenix (https://phoenix.apache.org/index.html) is a SQL layer over HBase.

This project aims to cover some aspects of usage of Apache Phoenix in Spark:

1) Create DataFrame as select from phoenix table by set of keys (or prefixes), stored in another DataFrame
2) Create DataFrame of arbitrary sql
3) Bulk-Load
4) Maintain materialized views (since phoenix's secondary indexes works correctly only in 1-node hbase cluster)
