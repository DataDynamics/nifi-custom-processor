package io.datadynamics.nifi.record.csv;

import org.apache.nifi.schema.access.SchemaAccessWriter;
import org.apache.nifi.serialization.AbstractRecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.*;
import shaded.org.apache.commons.csv.CSVFormat;
import shaded.org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class WriteCSVResult extends AbstractRecordSetWriter implements RecordSetWriter, RawRecordWriter {
    private final RecordSchema recordSchema;
    private final SchemaAccessWriter schemaWriter;
    private final String dateFormat;
    private final String timeFormat;
    private final String timestampFormat;
    private final CSVPrinter printer;
    private final Object[] fieldValues;
    private final boolean includeHeaderLine;
    private boolean headerWritten = false;
    private String[] fieldNames;

    public WriteCSVResult(final CSVFormat csvFormat, final RecordSchema recordSchema, final SchemaAccessWriter schemaWriter, final OutputStream out,
                          final String dateFormat, final String timeFormat, final String timestampFormat, final boolean includeHeaderLine, final String charSet) throws IOException {

        super(out);
        this.recordSchema = recordSchema;
        this.schemaWriter = schemaWriter;
        this.dateFormat = dateFormat;
        this.timeFormat = timeFormat;
        this.timestampFormat = timestampFormat;
        this.includeHeaderLine = includeHeaderLine;

        final CSVFormat formatWithHeader = csvFormat.withSkipHeaderRecord(true);
        final OutputStreamWriter streamWriter = new OutputStreamWriter(out, charSet);
        printer = new CSVPrinter(streamWriter, formatWithHeader);

        fieldValues = new Object[recordSchema.getFieldCount()];
    }

    private String getFormat(final RecordField field) {
        final DataType dataType = field.getDataType();
        switch (dataType.getFieldType()) {
            case DATE:
                return dateFormat;
            case TIME:
                return timeFormat;
            case TIMESTAMP:
                return timestampFormat;
        }

        return dataType.getFormat();
    }

    @Override
    protected void onBeginRecordSet() throws IOException {
        schemaWriter.writeHeader(recordSchema, getOutputStream());
    }

    @Override
    protected Map<String, String> onFinishRecordSet() throws IOException {
        // If the header has not yet been written (but should be), write it out now
        includeHeaderIfNecessary(null, true);
        return schemaWriter.getAttributes(recordSchema);
    }

    @Override
    public void close() throws IOException {
        printer.close();
    }

    @Override
    public void flush() throws IOException {
        printer.flush();
    }

    private String[] getFieldNames(final Record record) {
        if (fieldNames != null) {
            return fieldNames;
        }

        final Set<String> allFields = new LinkedHashSet<>();
        // The fields defined in the schema should be written first followed by extra ones.
        allFields.addAll(recordSchema.getFieldNames());
        allFields.addAll(record.getRawFieldNames());
        fieldNames = allFields.toArray(new String[0]);
        return fieldNames;
    }

    private void includeHeaderIfNecessary(final Record record, final boolean includeOnlySchemaFields) throws IOException {
        if (headerWritten || !includeHeaderLine) {
            return;
        }

        final Object[] fieldNames;
        if (includeOnlySchemaFields) {
            fieldNames = recordSchema.getFieldNames().toArray(new Object[0]);
        } else {
            fieldNames = getFieldNames(record);
        }

        printer.printRecord(fieldNames);
        headerWritten = true;
    }

    @Override
    public Map<String, String> writeRecord(final Record record) throws IOException {
        // If we are not writing an active record set, then we need to ensure that we write the
        // schema information.
        if (!isActiveRecordSet()) {
            schemaWriter.writeHeader(recordSchema, getOutputStream());
        }

        includeHeaderIfNecessary(record, true);

        int i = 0;
        for (final RecordField recordField : recordSchema.getFields()) {
            fieldValues[i++] = getFieldValue(record, recordField);
        }

        printer.printRecord(fieldValues);
        return schemaWriter.getAttributes(recordSchema);
    }

    private Object getFieldValue(final Record record, final RecordField recordField) {
        final RecordFieldType fieldType = recordField.getDataType().getFieldType();

        switch (fieldType) {
            case BIGINT:
            case BYTE:
            case DECIMAL:
            case DOUBLE:
            case FLOAT:
            case LONG:
            case INT:
            case SHORT:
                final Object value = record.getValue(recordField);
                if (value instanceof Number) {
                    return value;
                }
                break;
        }

        return record.getAsString(recordField, getFormat(recordField));
    }

    @Override
    public WriteResult writeRawRecord(final Record record) throws IOException {
        // If we are not writing an active record set, then we need to ensure that we write the
        // schema information.
        if (!isActiveRecordSet()) {
            schemaWriter.writeHeader(recordSchema, getOutputStream());
        }

        includeHeaderIfNecessary(record, false);

        final String[] fieldNames = getFieldNames(record);
        // Avoid creating a new Object[] for every Record if we can. But if the record has a different number of columns than does our
        // schema, we don't have a lot of options here, so we just create a new Object[] in that case.
        final Object[] recordFieldValues = (fieldNames.length == this.fieldValues.length) ? this.fieldValues : new String[fieldNames.length];

        int i = 0;
        for (final String fieldName : fieldNames) {
            final Optional<RecordField> recordField = recordSchema.getField(fieldName);
            if (recordField.isPresent()) {
                recordFieldValues[i++] = record.getAsString(fieldName, getFormat(recordField.get()));
            } else {
                recordFieldValues[i++] = record.getAsString(fieldName);
            }
        }

        printer.printRecord(recordFieldValues);
        final Map<String, String> attributes = schemaWriter.getAttributes(recordSchema);
        return WriteResult.of(incrementRecordCount(), attributes);
    }

    @Override
    public String getMimeType() {
        return "text/csv";
    }
}
