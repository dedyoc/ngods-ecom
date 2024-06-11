# Ecommerce Analytics demo
This repository contains an ecommerce analysis demo of the ngods data stack.

# ngods
ngods stands for New Generation Opensource Data Stack. It includes the following components: 

- [Apache Spark](https://spark.apache.org) for data transformation 
- [Apache Iceberg](https://iceberg.apache.org) as a data storage format 
- [Dagster](https://dagster.io/) for data orchetsration 
- [Metabase](https://www.metabase.com/) for self-service data visualization (dashboards) 
- [Minio](https://min.io) for local S3 storage 

![ngods components](./img/ngods.architecture.png)

ngods is open-sourced under a [BSD license](https://github.com/zsvoboda/ngods-stocks/blob/main/LICENSE) and it is distributed as a docker-compose script that supports Intel and ARM architectures.
