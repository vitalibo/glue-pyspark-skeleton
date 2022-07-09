package com.github.vitalibo.dataplatform.core.transform;


import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.functions;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

public class SampleTransform {
    /**
     * Demonstration of using Spark Dataset API
     */

    public static Dataset<Row> transform(Dataset<Row> df) {
        return df.mapPartitions(new FakeMapPartitions(), RowEncoder.apply(df.schema()))
            .withColumn("java", functions.lit(true));
    }

    private static class FakeMapPartitions implements MapPartitionsFunction<Row, Row> {

        @Override
        public Iterator<Row> call(Iterator<Row> iterator) {
            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
                .iterator();
        }
    }

}
