package org.apache.flink.connector.mongodb.table;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;

import java.io.IOException;

public class MongoDBDynamicOutputFormat extends RichOutputFormat<RowData> {

    @Override
    public void configure(Configuration parameters) {

    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {

    }

    @Override
    public void writeRecord(RowData record) throws IOException {

    }

    @Override
    public void close() throws IOException {

    }

}
