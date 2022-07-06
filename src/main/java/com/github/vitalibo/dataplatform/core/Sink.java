package com.github.vitalibo.dataplatform.core;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

@FunctionalInterface
public interface Sink {

    void load(Spark spark, Dataset<Row> df);

}
