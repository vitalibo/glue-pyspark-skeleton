package com.github.vitalibo.dataplatform.core;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

@FunctionalInterface
public interface Source {

    Dataset<Row> extract(Spark spark);

}
