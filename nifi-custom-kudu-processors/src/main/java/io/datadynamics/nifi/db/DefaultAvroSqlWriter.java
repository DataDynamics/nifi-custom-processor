package io.datadynamics.nifi.db;

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static io.datadynamics.nifi.db.JdbcCommon.AvroConversionOptions;
import static io.datadynamics.nifi.db.JdbcCommon.ResultSetRowCallback;

public class DefaultAvroSqlWriter implements SqlWriter {

    private final AvroConversionOptions options;

    private final Map<String, String> attributesToAdd = new HashMap<String, String>() {{
        put(CoreAttributes.MIME_TYPE.key(), JdbcCommon.MIME_TYPE_AVRO_BINARY);
    }};

    public DefaultAvroSqlWriter(AvroConversionOptions options) {
        this.options = options;
    }

    @Override
    public long writeResultSet(ResultSet resultSet, OutputStream outputStream, ComponentLog logger, ResultSetRowCallback callback) throws Exception {
        try {
            return JdbcCommon.convertToAvroStream(resultSet, outputStream, options, callback);
        } catch (SQLException e) {
            throw new ProcessException(e);
        }
    }

    @Override
    public Map<String, String> getAttributesToAdd() {
        return attributesToAdd;
    }

    @Override
    public void writeEmptyResultSet(OutputStream outputStream, ComponentLog logger) throws IOException {
        JdbcCommon.createEmptyAvroStream(outputStream);
    }

    @Override
    public String getMimeType() {
        return JdbcCommon.MIME_TYPE_AVRO_BINARY;
    }
}