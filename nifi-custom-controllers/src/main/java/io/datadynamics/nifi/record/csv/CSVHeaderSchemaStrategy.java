package io.datadynamics.nifi.record.csv;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.csv.CSVUtils;
import org.apache.nifi.schema.access.SchemaAccessStrategy;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.*;

public class CSVHeaderSchemaStrategy implements SchemaAccessStrategy {
    private static final Set<SchemaField> schemaFields = EnumSet.noneOf(SchemaField.class);

    private final PropertyContext context;

    public CSVHeaderSchemaStrategy(final PropertyContext context) {
        this.context = context;
    }

    @Override
    public RecordSchema getSchema(Map<String, String> variables, final InputStream contentStream, final RecordSchema readSchema) throws SchemaNotFoundException {
        if (this.context == null) {
            throw new SchemaNotFoundException("Schema Access Strategy intended only for validation purposes and cannot obtain schema");
        }

        try {
            final CSVFormat csvFormat = CSVUtils.createCSVFormat(context, variables).withFirstRecordAsHeader();
            try (final Reader reader = new InputStreamReader(new BOMInputStream(contentStream));
                final CSVParser csvParser = new CSVParser(reader, csvFormat)) {

                final List<RecordField> fields = new ArrayList<>();
                for (final String columnName : csvParser.getHeaderMap().keySet()) {
                    fields.add(new RecordField(columnName, RecordFieldType.STRING.getDataType(), true));
                }

                return new SimpleRecordSchema(fields);
            }
        } catch (final Exception e) {
            throw new SchemaNotFoundException("Failed to read Header line from CSV", e);
        }
    }

    @Override
    public Set<SchemaField> getSuppliedSchemaFields() {
        return schemaFields;
    }
}
