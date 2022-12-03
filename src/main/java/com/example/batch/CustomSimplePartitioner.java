package com.example.batch;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;

import java.util.HashMap;
import java.util.Map;

public class CustomSimplePartitioner implements Partitioner {

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        Map<String,ExecutionContext> partitions= new HashMap<>();
       // int gr=4;
       for (int k=1;k<=gridSize;k++){
           ExecutionContext executionContext= new ExecutionContext();
           executionContext.put("partitionNum",k);
           partitions.put(String.valueOf(k),executionContext);
       }
       return partitions;
    }
}
