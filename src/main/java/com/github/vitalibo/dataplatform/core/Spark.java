package com.github.vitalibo.dataplatform.core;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class Spark {

    public void submit(Job job) {
        job.transform(this);
    }

    public Dataset<Row> extract(Source source) {
        return source.extract(this);
    }

    public void load(Sink sink, Dataset<Row> df) {
        sink.load(this, df);
    }

}
