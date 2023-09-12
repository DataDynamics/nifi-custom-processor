package io.datadynamics.nifi.record.csv;

import org.apache.commons.io.input.BOMInputStream;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.*;
import shaded.org.apache.commons.csv.CSVFormat;
import shaded.org.apache.commons.csv.CSVParser;
import shaded.org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.*;

public class CSVRecordReader extends AbstractCSVRecordReader {
    private final CSVParser csvParser;

    private List<RecordField> recordFields;

    public CSVRecordReader(final InputStream in, final ComponentLog logger, final RecordSchema schema, final CSVFormat csvFormat, final boolean hasHeader, final boolean ignoreHeader,
                           final String dateFormat, final String timeFormat, final String timestampFormat, final String encoding, final boolean trimDoubleQuote, Integer fieldCount, boolean failOnMismatchFieldCount) throws IOException {
        super(logger, schema, hasHeader, ignoreHeader, dateFormat, timeFormat, timestampFormat, trimDoubleQuote, fieldCount, failOnMismatchFieldCount);

        final Reader reader = new InputStreamReader(new BOMInputStream(in), encoding);

        CSVFormat withHeader;
        if (hasHeader) {
            withHeader = csvFormat.withSkipHeaderRecord();

            if (ignoreHeader) {
                withHeader = withHeader.withHeader(schema.getFieldNames().toArray(new String[0]));
            } else {
                withHeader = withHeader.withFirstRecordAsHeader();
            }
        } else {
            withHeader = csvFormat.withHeader(schema.getFieldNames().toArray(new String[0]));
        }

        withHeader = csvFormat.withFieldCount(fieldCount).withValidateFieldCount(failOnMismatchFieldCount);

        csvParser = new CSVParser(reader, withHeader);
    }

    public CSVRecordReader(final InputStream in, final ComponentLog logger, final RecordSchema schema, final CSVFormat csvFormat, final boolean hasHeader, final boolean ignoreHeader,
                           final String dateFormat, final String timeFormat, final String timestampFormat, final String encoding, Integer fieldCount, boolean failOnMismatchFieldCount) throws IOException {
        this(in, logger, schema, csvFormat, hasHeader, ignoreHeader, dateFormat, timeFormat, timestampFormat, encoding, true, fieldCount, failOnMismatchFieldCount);
    }

    @Override
    public Record nextRecord(final boolean coerceTypes, final boolean dropUnknownFields) throws IOException, MalformedRecordException {
        try {
            final RecordSchema schema = getSchema();

            final List<RecordField> recordFields = getRecordFields();
            final int numFieldNames = recordFields.size();
            for (final CSVRecord csvRecord : csvParser) {
                final Map<String, Object> values = new LinkedHashMap<>(recordFields.size() * 2);
                for (int i = 0; i < csvRecord.size(); i++) {
                    final String rawValue = csvRecord.get(i);

                    final String rawFieldName;
                    final DataType dataType;
                    if (i >= numFieldNames) {
                        if (!dropUnknownFields) {
                            values.put("unknown_field_index_" + i, rawValue);
                        }

                        continue;
                    } else {
                        final RecordField recordField = recordFields.get(i);
                        rawFieldName = recordField.getFieldName();
                        dataType = recordField.getDataType();
                    }


                    final Object value;
                    if (coerceTypes) {
                        value = convert(rawValue, dataType, rawFieldName);
                    } else {
                        // The CSV Reader is going to return all fields as Strings, because CSV doesn't have any way to
                        // dictate a field type. As a result, we will use the schema that we have to attempt to convert
                        // the value into the desired type if it's a simple type.
                        value = convertSimpleIfPossible(rawValue, dataType, rawFieldName);
                    }

                    values.put(rawFieldName, value);
                }

                return new MapRecord(schema, values, coerceTypes, dropUnknownFields);
            }
        } catch (Exception e) {
            throw new MalformedRecordException("Error while getting next record", e);
        }

        return null;
    }


    private List<RecordField> getRecordFields() {
        if (this.recordFields != null) {
            return this.recordFields;
        }

        // Use a SortedMap keyed by index of the field so that we can get a List of field names in the correct order
        final SortedMap<Integer, String> sortedMap = new TreeMap<>();
        for (final Map.Entry<String, Integer> entry : csvParser.getHeaderMap().entrySet()) {
            sortedMap.put(entry.getValue(), entry.getKey());
        }

        final List<RecordField> fields = new ArrayList<>();
        final List<String> rawFieldNames = new ArrayList<>(sortedMap.values());
        for (final String rawFieldName : rawFieldNames) {
            final Optional<RecordField> option = schema.getField(rawFieldName);
            if (option.isPresent()) {
                fields.add(option.get());
            } else {
                fields.add(new RecordField(rawFieldName, RecordFieldType.STRING.getDataType()));
            }
        }

        this.recordFields = fields;
        return fields;
    }

    @Override
    public void close() throws IOException {
        csvParser.close();
    }
}
