package org.apache.flink.connector.mongodb.table;

import org.apache.flink.core.io.InputSplit;

public class MongoDBTableInputSplit implements InputSplit, java.io.Serializable {

    @Override
    public int getSplitNumber() {
        return 0;
    }

}
