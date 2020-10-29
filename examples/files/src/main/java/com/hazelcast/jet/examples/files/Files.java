package com.hazelcast.jet.examples.files;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.file.FileSources;

public class Files {

/*
    public static void main(String[] args) {


        Sources.map
        Pipeline p = Pipeline.create();
        p.readFrom(FileSources.files("src/main/java")
                              .build())
         .filter(line -> !line.isEmpty())
         .aggregate(AggregateOperations.counting())
         .
         .writeTo(Sinks.logger());

        Jet.bootstrappedInstance();''
    }*/
}
