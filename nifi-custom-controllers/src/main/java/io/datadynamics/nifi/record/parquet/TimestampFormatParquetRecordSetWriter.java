package io.datadynamics.nifi.record.parquet;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.avro.Schema;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SchemaRegistryRecordSetWriter;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Tags({"custom", "timestamp", "datadynamics", "parquet", "result", "set", "writer", "serializer", "record", "recordset", "row"})
@CapabilityDescription("Writes the contents of a RecordSet in Parquet format.")
public class TimestampFormatParquetRecordSetWriter extends SchemaRegistryRecordSetWriter implements RecordSetWriterFactory {

    public static final PropertyDescriptor CACHE_SIZE = new PropertyDescriptor.Builder()
            .name("cache-size")
            .displayName("Cache Size")
            .description("Specifies how many Schemas should be cached")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("1000")
            .required(true)
            .build();

    public static final PropertyDescriptor INT96_FIELDS = new PropertyDescriptor.Builder()
            .name("int96-fields")
            .displayName("INT96 Fields")
            .description("List of fields with full path that should be treated as INT96 timestamps.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(false)
            .build();

    public static final PropertyDescriptor TIMESTAMP_FORMAT_PROPERTY_NAME = new PropertyDescriptor.Builder()
            .name("timestamp-format-property-name")
            .displayName("Avro Schema Properties Name for Timestamp Format")
            .description("Timestamp 컬럼을 처리할 때 사용할 Format을 정의한 Avro Schema의 Property Name (기본값; properties)")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("properties")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();

    public static final PropertyDescriptor ADD_HOURS = new PropertyDescriptor.Builder()
            .name("add-hours")
            .displayName("Add Hours")
            .description("Timestamp 컬럼에 시간을 +합니다.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .required(false)
            .build();

    private LoadingCache<String, Schema> compiledAvroSchemaCache;
    private String int96Fields;
    private String timestampFormatPropertyKeyName;
    private int addHours;

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        final int cacheSize = context.getProperty(CACHE_SIZE).asInteger();
        compiledAvroSchemaCache = Caffeine.newBuilder()
                .maximumSize(cacheSize)
                .build(schemaText -> new Schema.Parser().parse(schemaText));

        if (context.getProperty(INT96_FIELDS).isSet()) {
            int96Fields = context.getProperty(INT96_FIELDS).getValue();
        } else {
            int96Fields = null;
        }

        if (context.getProperty(TIMESTAMP_FORMAT_PROPERTY_NAME).isSet()) { // FIXED
            timestampFormatPropertyKeyName = context.getProperty(TIMESTAMP_FORMAT_PROPERTY_NAME).evaluateAttributeExpressions().getValue();
        } else {
            timestampFormatPropertyKeyName = "properties";
        }

        if (context.getProperty(ADD_HOURS).isSet()) { // FIXED
            addHours = Integer.parseInt(context.getProperty(ADD_HOURS).evaluateAttributeExpressions().getValue());
        } else {
            addHours = 0;
        }

        if (getLogger().isInfoEnabled()) {
            getLogger().info("[DFM] TimestampFormatParquetRecordSetWriter : Timestamp Pattern Property Key Name = {}, Add Hour = {}", timestampFormatPropertyKeyName, addHours);
        }
    }

    @Override
    public RecordSetWriter createWriter(final ComponentLog logger, final RecordSchema recordSchema,
                                        final OutputStream out, final Map<String, String> variables) throws IOException {
        final ParquetConfig parquetConfig = ParquetUtils.createParquetConfig(getConfigurationContext(), variables);
        parquetConfig.setInt96Fields(int96Fields);

        try {
            final Schema avroSchema;
            try {
                if (recordSchema.getSchemaFormat().isPresent() && recordSchema.getSchemaFormat().get().equals(AvroTypeUtil.AVRO_SCHEMA_FORMAT)) {
                    final Optional<String> textOption = recordSchema.getSchemaText();
                    if (textOption.isPresent()) {
                        avroSchema = compiledAvroSchemaCache.get(textOption.get());
                    } else {
                        avroSchema = AvroTypeUtil.extractAvroSchema(recordSchema);
                    }
                } else {
                    avroSchema = AvroTypeUtil.extractAvroSchema(recordSchema);
                }
            } catch (final Exception e) {
                throw new SchemaNotFoundException("Failed to compile Avro Schema", e);
            }

            return new WriteParquetResult(avroSchema, recordSchema, getSchemaAccessWriter(recordSchema, variables), out, parquetConfig, logger, timestampFormatPropertyKeyName, addHours);

        } catch (final SchemaNotFoundException e) {
            throw new ProcessException("Could not determine the Avro Schema to use for writing the content", e);
        }
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(CACHE_SIZE);
        properties.add(ParquetUtils.COMPRESSION_TYPE);
        properties.add(ParquetUtils.ROW_GROUP_SIZE);
        properties.add(ParquetUtils.PAGE_SIZE);
        properties.add(ParquetUtils.DICTIONARY_PAGE_SIZE);
        properties.add(ParquetUtils.MAX_PADDING_SIZE);
        properties.add(ParquetUtils.ENABLE_DICTIONARY_ENCODING);
        properties.add(ParquetUtils.ENABLE_VALIDATION);
        properties.add(ParquetUtils.WRITER_VERSION);
        properties.add(ParquetUtils.AVRO_WRITE_OLD_LIST_STRUCTURE);
        properties.add(ParquetUtils.AVRO_ADD_LIST_ELEMENT_RECORDS);
        properties.add(TIMESTAMP_FORMAT_PROPERTY_NAME);
        properties.add(ADD_HOURS);
        properties.add(INT96_FIELDS);
        return properties;
    }
}
