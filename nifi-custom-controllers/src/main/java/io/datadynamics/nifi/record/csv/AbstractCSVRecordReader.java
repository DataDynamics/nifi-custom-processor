package io.datadynamics.nifi.record.csv;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.text.DateFormat;
import java.util.function.Supplier;

abstract public class AbstractCSVRecordReader implements RecordReader {

    protected final ComponentLog logger;

    protected final boolean hasHeader;

    protected final boolean ignoreHeader;
    protected final Supplier<DateFormat> LAZY_DATE_FORMAT;
    protected final Supplier<DateFormat> LAZY_TIME_FORMAT;
    protected final Supplier<DateFormat> LAZY_TIMESTAMP_FORMAT;
    protected final String dateFormat;
    protected final String timeFormat;
    protected final String timestampFormat;
    protected final RecordSchema schema;
    final Integer fieldCount;
    final boolean failOnMismatchFieldCount;
    private final boolean trimDoubleQuote;

    AbstractCSVRecordReader(final ComponentLog logger, final RecordSchema schema, final boolean hasHeader, final boolean ignoreHeader,
                            final String dateFormat, final String timeFormat, final String timestampFormat, final boolean trimDoubleQuote) {
        this.logger = logger;
        this.schema = schema;
        this.hasHeader = hasHeader;
        this.ignoreHeader = ignoreHeader;
        this.trimDoubleQuote = trimDoubleQuote;
        this.fieldCount = -1;
        this.failOnMismatchFieldCount = false;

        if (dateFormat == null || dateFormat.isEmpty()) {
            this.dateFormat = null;
            LAZY_DATE_FORMAT = null;
        } else {
            this.dateFormat = dateFormat;
            LAZY_DATE_FORMAT = () -> DataTypeUtils.getDateFormat(dateFormat);
        }

        if (timeFormat == null || timeFormat.isEmpty()) {
            this.timeFormat = null;
            LAZY_TIME_FORMAT = null;
        } else {
            this.timeFormat = timeFormat;
            LAZY_TIME_FORMAT = () -> DataTypeUtils.getDateFormat(timeFormat);
        }

        if (timestampFormat == null || timestampFormat.isEmpty()) {
            this.timestampFormat = null;
            LAZY_TIMESTAMP_FORMAT = null;
        } else {
            this.timestampFormat = timestampFormat;
            LAZY_TIMESTAMP_FORMAT = () -> DataTypeUtils.getDateFormat(timestampFormat);
        }
    }

    AbstractCSVRecordReader(final ComponentLog logger, final RecordSchema schema, final boolean hasHeader, final boolean ignoreHeader,
                            final String dateFormat, final String timeFormat, final String timestampFormat, final boolean trimDoubleQuote, Integer fieldCount, boolean failOnMismatchFieldCount) {
        this.logger = logger;
        this.schema = schema;
        this.hasHeader = hasHeader;
        this.ignoreHeader = ignoreHeader;
        this.trimDoubleQuote = trimDoubleQuote;
        this.fieldCount = fieldCount;
        this.failOnMismatchFieldCount = failOnMismatchFieldCount;

        if (dateFormat == null || dateFormat.isEmpty()) {
            this.dateFormat = null;
            LAZY_DATE_FORMAT = null;
        } else {
            this.dateFormat = dateFormat;
            LAZY_DATE_FORMAT = () -> DataTypeUtils.getDateFormat(dateFormat);
        }

        if (timeFormat == null || timeFormat.isEmpty()) {
            this.timeFormat = null;
            LAZY_TIME_FORMAT = null;
        } else {
            this.timeFormat = timeFormat;
            LAZY_TIME_FORMAT = () -> DataTypeUtils.getDateFormat(timeFormat);
        }

        if (timestampFormat == null || timestampFormat.isEmpty()) {
            this.timestampFormat = null;
            LAZY_TIMESTAMP_FORMAT = null;
        } else {
            this.timestampFormat = timestampFormat;
            LAZY_TIMESTAMP_FORMAT = () -> DataTypeUtils.getDateFormat(timestampFormat);
        }
    }

    protected final Object convert(final String value, final DataType dataType, final String fieldName) {
        if (dataType == null || value == null) {
            return value;
        }

        final String trimmed;
        final RecordFieldType type = dataType.getFieldType();

        if (!trimDoubleQuote && (type.equals(RecordFieldType.STRING) || type.equals(RecordFieldType.CHOICE))) {
            trimmed = value;
        } else {
            trimmed = trim(value);
        }

        if (trimmed.isEmpty()) {
            return null;
        }

        return DataTypeUtils.convertType(trimmed, dataType, LAZY_DATE_FORMAT, LAZY_TIME_FORMAT, LAZY_TIMESTAMP_FORMAT, fieldName);
    }

    protected final Object convertSimpleIfPossible(final String value, final DataType dataType, final String fieldName) {
        if (dataType == null || value == null) {
            return value;
        }

        final String trimmed;

        if (!trimDoubleQuote && dataType.getFieldType().equals(RecordFieldType.STRING)) {
            trimmed = value;
        } else {
            trimmed = trim(value);
        }

        if (trimmed.isEmpty()) {
            return null;
        }

        switch (dataType.getFieldType()) {
            case STRING:
                return value;
            case BOOLEAN:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
            case BYTE:
            case CHAR:
            case SHORT:
                if (DataTypeUtils.isCompatibleDataType(trimmed, dataType)) {
                    return DataTypeUtils.convertType(trimmed, dataType, LAZY_DATE_FORMAT, LAZY_TIME_FORMAT, LAZY_TIMESTAMP_FORMAT, fieldName);
                }
                break;
            case DATE:
                if (DataTypeUtils.isDateTypeCompatible(trimmed, dateFormat)) {
                    return DataTypeUtils.convertType(trimmed, dataType, LAZY_DATE_FORMAT, LAZY_TIME_FORMAT, LAZY_TIMESTAMP_FORMAT, fieldName);
                }
                break;
            case TIME:
                if (DataTypeUtils.isTimeTypeCompatible(trimmed, timeFormat)) {
                    return DataTypeUtils.convertType(trimmed, dataType, LAZY_DATE_FORMAT, LAZY_TIME_FORMAT, LAZY_TIMESTAMP_FORMAT, fieldName);
                }
                break;
            case TIMESTAMP:
                if (DataTypeUtils.isTimestampTypeCompatible(trimmed, timestampFormat)) {
                    return DataTypeUtils.convertType(trimmed, dataType, LAZY_DATE_FORMAT, LAZY_TIME_FORMAT, LAZY_TIMESTAMP_FORMAT, fieldName);
                }
                break;
        }

        return value;
    }

    private String trim(String value) {
        return (value.length() > 1) && value.startsWith("\"") && value.endsWith("\"") ? value.substring(1, value.length() - 1) : value;
    }

    @Override
    public RecordSchema getSchema() {
        return schema;
    }
}
