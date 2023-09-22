package io.datadynamics.nifi.record.avro;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.datadynamics.nifi.record.parquet.AvroTypeUtil;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.io.BinaryEncoder;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SchemaRegistryRecordSetWriter;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Tags({"custom", "timestamp", "datadynamics", "avro", "result", "set", "writer", "serializer", "record", "recordset", "row"})
@CapabilityDescription("Writes the contents of a RecordSet in Binary Avro format.")
public class TimestampFormatAvroRecordSetWriter extends SchemaRegistryRecordSetWriter implements RecordSetWriterFactory {
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
    static final PropertyDescriptor ENCODER_POOL_SIZE = new Builder()
            .name("encoder-pool-size")
            .displayName("Encoder Pool Size")
            .description("Avro Writers require the use of an Encoder. Creation of Encoders is expensive, but once created, they can be reused. This property controls the maximum number of Encoders that" +
                    " can be pooled and reused. Setting this value too small can result in degraded performance, but setting it higher can result in more heap being used. This property is ignored if the" +
                    " Avro Writer is configured with a Schema Write Strategy of 'Embed Avro Schema'.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("32")
            .build();
    static final AllowableValue AVRO_EMBEDDED = new AllowableValue("avro-embedded", "Embed Avro Schema",
            "The FlowFile will have the Avro schema embedded into the content, as is typical with Avro");
    static final PropertyDescriptor CACHE_SIZE = new PropertyDescriptor.Builder()
            .name("cache-size")
            .displayName("Cache Size")
            .description("Specifies how many Schemas should be cached")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("1000")
            .required(true)
            .build();
    private static final Set<SchemaField> requiredSchemaFields = EnumSet.of(SchemaField.SCHEMA_TEXT, SchemaField.SCHEMA_TEXT_FORMAT);
    private static final PropertyDescriptor COMPRESSION_FORMAT = new Builder()
            .name("compression-format")
            .displayName("Compression Format")
            .description("Compression type to use when writing Avro files. Default is None.")
            .allowableValues(CodecType.values())
            .defaultValue(CodecType.NONE.toString())
            .required(true)
            .build();
    private LoadingCache<String, Schema> compiledAvroSchemaCache;
    private volatile BlockingQueue<BinaryEncoder> encoderPool;
    private String timestampFormatPropertyKeyName;
    private int addHours;

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        final int cacheSize = context.getProperty(CACHE_SIZE).asInteger();
        compiledAvroSchemaCache = Caffeine.newBuilder()
                .maximumSize(cacheSize)
                .build(schemaText -> new Schema.Parser().parse(schemaText));

        final int capacity = context.getProperty(ENCODER_POOL_SIZE).evaluateAttributeExpressions().asInteger();
        encoderPool = new LinkedBlockingQueue<>(capacity);

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

        if (getLogger().isDebugEnabled()) {
            getLogger().debug("[DFM] TimestampFormatAvroRecordSetWriter : Timestamp Pattern Property Key Name = {}, Add Hour = {}", timestampFormatPropertyKeyName, addHours);
        }
    }

    @OnDisabled
    public void cleanup() {
        if (encoderPool != null) {
            encoderPool.clear();
        }
    }

    @Override
    public RecordSetWriter createWriter(final ComponentLog logger, final RecordSchema recordSchema, final OutputStream out, final Map<String, String> variables) throws IOException {
        final String strategyValue = getConfigurationContext().getProperty(getSchemaWriteStrategyDescriptor()).getValue();
        final String compressionFormat = getConfigurationContext().getProperty(COMPRESSION_FORMAT).getValue();

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

            if (AVRO_EMBEDDED.getValue().equals(strategyValue)) {
                return new WriteAvroResultWithSchema(avroSchema, out, getCodecFactory(compressionFormat), timestampFormatPropertyKeyName, addHours);
            } else {
                return new WriteAvroResultWithExternalSchema(avroSchema, recordSchema, getSchemaAccessWriter(recordSchema, variables), out, encoderPool, getLogger(), timestampFormatPropertyKeyName, addHours);
            }
        } catch (final SchemaNotFoundException e) {
            throw new ProcessException("Could not determine the Avro Schema to use for writing the content", e);
        }
    }

    private CodecFactory getCodecFactory(String property) {
        CodecType type = CodecType.valueOf(property);
        switch (type) {
            case BZIP2:
                return CodecFactory.bzip2Codec();
            case DEFLATE:
                return CodecFactory.deflateCodec(CodecFactory.DEFAULT_DEFLATE_LEVEL);
            case LZO:
                return CodecFactory.xzCodec(CodecFactory.DEFAULT_XZ_LEVEL);
            case SNAPPY:
                return CodecFactory.snappyCodec();
            case NONE:
            default:
                return CodecFactory.nullCodec();
        }
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(COMPRESSION_FORMAT);
        properties.add(CACHE_SIZE);
        properties.add(ENCODER_POOL_SIZE);
        properties.add(TIMESTAMP_FORMAT_PROPERTY_NAME);
        properties.add(ADD_HOURS);
        return properties;
    }

    @Override
    protected List<AllowableValue> getSchemaWriteStrategyValues() {
        final List<AllowableValue> allowableValues = new ArrayList<>();
        allowableValues.add(AVRO_EMBEDDED);
        allowableValues.addAll(super.getSchemaWriteStrategyValues());
        return allowableValues;
    }

    @Override
    protected AllowableValue getDefaultSchemaWriteStrategy() {
        return AVRO_EMBEDDED;
    }

    @Override
    protected Set<SchemaField> getRequiredSchemaFields(final ValidationContext validationContext) {
        final String writeStrategyValue = validationContext.getProperty(getSchemaWriteStrategyDescriptor()).getValue();
        if (writeStrategyValue.equalsIgnoreCase(AVRO_EMBEDDED.getValue())) {
            return requiredSchemaFields;
        }

        return super.getRequiredSchemaFields(validationContext);
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(validationContext));
        final String writeStrategyValue = validationContext.getProperty(getSchemaWriteStrategyDescriptor()).getValue();
        final String compressionFormatValue = validationContext.getProperty(COMPRESSION_FORMAT).getValue();
        if (!writeStrategyValue.equalsIgnoreCase(AVRO_EMBEDDED.getValue())
                && !CodecType.NONE.toString().equals(compressionFormatValue)) {
            results.add(new ValidationResult.Builder()
                    .subject(COMPRESSION_FORMAT.getName())
                    .valid(false)
                    .explanation("Avro compression codecs are stored in the header of the Avro file and therefore "
                            + "requires the header to be embedded into the content.")
                    .build());
        }

        return results;
    }

    private enum CodecType {
        BZIP2,
        DEFLATE,
        NONE,
        SNAPPY,
        LZO
    }
}
