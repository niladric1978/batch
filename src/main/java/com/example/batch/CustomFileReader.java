package com.example.batch;

import org.springframework.batch.item.ItemCountAware;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.file.FlatFileItemReader;

import static java.util.Objects.isNull;

public class CustomFileReader<Transaction> extends FlatFileItemReader<com.example.batch.Transaction>
        implements ItemReader<com.example.batch.Transaction> {
    private int size;
    private int currentItemCount=0;
    private int num= 0;
    public CustomFileReader (Integer gridSize, Integer pNum){
        size=gridSize;
        num=pNum;
    }
    public com.example.batch.Transaction read() throws Exception, UnexpectedInputException, ParseException {
       // if (currentItemCount >= maxItemCount) {
       //     return null;
       // }
        currentItemCount++;

        com.example.batch.Transaction item = doRead();

        if ((isNull(item))) {
            return null;
        }
            while (!(item.getUserId() % size == num-1)) {
                item = doRead();
                if ((isNull(item))) {
                    return null;
                }
            }
        if (item instanceof ItemCountAware) {
            ((ItemCountAware) item).setItemCount(currentItemCount);
        }
        System.out.println("item & partition ="+item + num);
        return item;

    }

}
