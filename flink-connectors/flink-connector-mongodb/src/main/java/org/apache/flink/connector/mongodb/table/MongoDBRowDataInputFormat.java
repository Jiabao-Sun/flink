package org.apache.flink.connector.mongodb.table;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.mongodb.converter.BsonRowConverter;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.data.RowData;

import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/** InputFormat for {@link MongoDBDynamicTableSource}. */
public class MongoDBRowDataInputFormat extends RichInputFormat<RowData, InputSplit>
        implements ResultTypeQueryable<RowData> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(MongoDBRowDataInputFormat.class);

    private FindIterable<BsonDocument> find;
    private int fetchSize;
    private Object[][] parameterValues;
    private BsonRowConverter rowConverter;
    private TypeInformation<RowData> rowDataTypeInfo;
    private transient boolean hasNext;
    private transient MongoCursor<BsonDocument> cursor;

    @Override
    public void configure(Configuration parameters) {
        // do nothing here
    }

    @Override
    public void openInputFormat() {
        // called once per inputFormat (on open)
        find.batchSize(fetchSize);
        // TODO
    }

    @Override
    public void closeInputFormat() {
        if (find != null) {
            find = null;
        }
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
        return cachedStatistics;
    }

    @Override
    public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
        if (parameterValues == null) {
            return new GenericInputSplit[] {new GenericInputSplit(0, 1)};
        }
        GenericInputSplit[] ret = new GenericInputSplit[parameterValues.length];
        for (int i = 0; i < ret.length; i++) {
            ret[i] = new GenericInputSplit(i, ret.length);
        }
        return ret;
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }

    @Override
    public void open(InputSplit split) throws IOException {
        cursor = find.cursor();
    }

    @Override
    public void close() throws IOException {
        if (cursor != null) {
            cursor.close();
        }
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return !hasNext;
    }

    @Override
    public RowData nextRecord(RowData reuse) throws IOException {
        if (!hasNext) {
            return null;
        }

        if (hasNext = cursor.hasNext()) {
            return rowConverter.toInternal(cursor.next());
        } else {
            return null;
        }
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return rowDataTypeInfo;
    }

}
