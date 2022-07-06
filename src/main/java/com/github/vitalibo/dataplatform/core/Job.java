package com.github.vitalibo.dataplatform.core;

@FunctionalInterface
public interface Job {

    void transform(Spark spark);

}

