package com.github.vitalibo.dataplatform.core;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

@FunctionalInterface
public interface Source {

    Dataset<Row> extract(SparkSession session);

}
