package org.apache.flink.connector.mongodb.converter;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.data.RowData;

import org.bson.BsonDocument;

import java.io.Serializable;
import java.sql.ResultSet;

/**
 * Converter that is responsible to convert between Bson object and Flink SQL internal data
 * structure {@link RowData}.
 */
@PublicEvolving
public interface BsonRowConverter extends Serializable {

    /**
     * Convert data retrieved from {@link ResultSet} to internal {@link RowData}.
     *
     * @param document BsonDocument from MongoDB
     */
    RowData toInternal(BsonDocument document);

    /**
     * Convert data retrieved from Flink internal RowData to Bson Object.
     *
     * @param rowData The given internal {@link RowData}.
     * @return The bson document.
     */
    BsonDocument toExternal(RowData rowData);

}
