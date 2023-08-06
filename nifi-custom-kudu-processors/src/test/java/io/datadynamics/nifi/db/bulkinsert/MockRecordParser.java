package io.datadynamics.nifi.db.bulkinsert;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class MockRecordParser extends AbstractControllerService implements RecordReaderFactory {
    private final List records = new ArrayList<>();
    private final List<RecordField> fields = new ArrayList<>();
    private int failAfterN;
    private MockRecordFailureType failureType = MockRecordFailureType.MALFORMED_RECORD_EXCEPTION;

    public MockRecordParser() {
        this(-1);
    }

    public MockRecordParser(final int failAfterN) {
        this.failAfterN = failAfterN;
    }

    public void failAfter(final int failAfterN) {
        failAfter(failAfterN, MockRecordFailureType.MALFORMED_RECORD_EXCEPTION);
    }

    public void failAfter(final int failAfterN, final MockRecordFailureType failureType) {
        this.failAfterN = failAfterN;
        this.failureType = failureType;
    }

    public void addSchemaField(final String fieldName, final RecordFieldType type) {
        addSchemaField(fieldName, type, RecordField.DEFAULT_NULLABLE);
    }

    public void addSchemaField(final String fieldName, final RecordFieldType type, boolean isNullable) {
        fields.add(new RecordField(fieldName, type.getDataType(), isNullable));
    }

    public void addSchemaField(final RecordField recordField) {
        fields.add(recordField);
    }

    public void addRecord(Object... values) {
        records.add(values);
    }

    @Override
    public RecordReader createRecordReader(Map<String, String> variables, InputStream in, long inputLength, ComponentLog logger) throws IOException, SchemaNotFoundException {
        final Iterator itr = records.iterator();

        return new RecordReader() {
            private int recordCount = 0;

            @Override
            public void close() throws IOException {
            }

            @Override
            public Record nextRecord(final boolean coerceTypes, final boolean dropUnknown) throws IOException, MalformedRecordException {
                if (failAfterN >= 0 && recordCount >= failAfterN) {
                    if (failureType == MockRecordFailureType.MALFORMED_RECORD_EXCEPTION) {
                        throw new MalformedRecordException("Intentional Unit Test Exception because " + recordCount + " records have been read");
                    } else {
                        throw new IOException("Intentional Unit Test Exception because " + recordCount + " records have been read");
                    }
                }
                recordCount++;

                if (!itr.hasNext()) {
                    return null;
                }

                final Object[] objects = (Object[]) itr.next();
                GenericRecord record = (GenericRecord) objects[0];
                final Map<String, Object> valueMap = new HashMap<>();
                int i = 0;
                for (final RecordField field : fields) {

                    final String fieldName = field.getFieldName();
                    List<Schema.Field> flds = record.getSchema().getFields();
                    int fieldIndex = 0;
                    for (Schema.Field fld : flds) {
                        if (fld.name().equals(fieldName)) {
                            break;
                        }
                        fieldIndex ++;
                    }
                    valueMap.put(fieldName, record.get(fieldIndex));
                }

                return new MapRecord(new SimpleRecordSchema(fields), valueMap);
            }

            @Override
            public RecordSchema getSchema() {
                return new SimpleRecordSchema(fields);
            }
        };
    }
}