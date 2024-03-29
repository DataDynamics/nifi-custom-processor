package io.datadynamics.nifi.record.csv;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaAccessStrategy;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schema.inference.*;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.serialization.DateTimeUtils;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.SchemaRegistryService;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.stream.io.NonCloseableInputStream;
import shaded.org.apache.commons.csv.CSVFormat;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Tags({"csv", "parse", "record", "row", "reader", "delimited", "comma", "separated", "values"})
@CapabilityDescription("Parses CSV-formatted data, returning each row in the CSV file as a separate record. "
        + "This reader allows for inferring a schema based on the first line of the CSV, if a 'header line' is present, or providing an explicit schema "
        + "for interpreting the values. See Controller Service's Usage for further documentation.")
public class CSVReader extends SchemaRegistryService implements RecordReaderFactory {

    // CSV parsers
    public static final AllowableValue APACHE_COMMONS_CSV = new AllowableValue("commons-csv", "Apache Commons CSV",
            "The CSV parser implementation from the Apache Commons CSV library.");
    public static final AllowableValue JACKSON_CSV = new AllowableValue("jackson-csv", "Jackson CSV",
            "The CSV parser implementation from the Jackson Dataformats library.");
    public static final PropertyDescriptor CSV_PARSER = new PropertyDescriptor.Builder()
            .name("csv-reader-csv-parser")
            .displayName("CSV Parser")
            .description("Specifies which parser to use to read CSV records. NOTE: Different parsers may support different subsets of functionality "
                    + "and may also exhibit different levels of performance.")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues(APACHE_COMMONS_CSV, JACKSON_CSV)
            .defaultValue(APACHE_COMMONS_CSV.getValue())
            .required(true)
            .build();
    public static final PropertyDescriptor TRIM_DOUBLE_QUOTE = new PropertyDescriptor.Builder()
            .name("Trim double quote")
            .description("Whether or not to trim starting and ending double quotes. For example: with trim string '\"test\"'"
                    + " would be parsed to 'test', without trim would be parsed to '\"test\"'."
                    + "If set to 'false' it means full compliance with RFC-4180. Default value is true, with trim.")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues("true", "false")
            .defaultValue("true")
            .dependsOn(CSVUtils.CSV_FORMAT, CSVUtils.RFC_4180)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .required(true)
            .build();
    public static final PropertyDescriptor FIELD_COUNT = new PropertyDescriptor.Builder()
            .name("필드(컬럼) 개수")
            .description("유효성 검사를 위한 CSV 파일의 필드(컬럼) 개수")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .required(false)
            .build();
    public static final PropertyDescriptor FAIL_ON_MISMATCH_FIELD_COUNT = new PropertyDescriptor.Builder()
            .name("필드(컬럼) 개수 불일치시 실패처리")
            .description("CSV 파일의 필드(컬럼) 개수가 지정한 필드 개수와 불일치 하는 경우 CSV 파일 실패 처리")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .allowableValues("true", "false")
            .defaultValue("false")
            .dependsOn(FIELD_COUNT)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .required(false)
            .build();
    public static final PropertyDescriptor USE_SCHEMA_FOR_FIELD_COUNT = new PropertyDescriptor.Builder()
            .name("필드(컬럼) 개수로 스키마를 활용")
            .description("별도로 필드(컬럼) 개수를 지정하지 않고 스키마로 필드 개수를 사용")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .required(false)
            .build();
    private static final AllowableValue HEADER_DERIVED = new AllowableValue("csv-header-derived", "Use String Fields From Header",
            "The first non-comment line of the CSV file is a header line that contains the names of the columns. The schema will be derived by using the "
                    + "column names in the header and assuming that all columns are of type String.");
    private volatile ConfigurationContext context;

    private volatile String csvParser;
    private volatile String dateFormat;
    private volatile String timeFormat;
    private volatile String timestampFormat;
    private volatile boolean firstLineIsHeader;
    private volatile boolean ignoreHeader;
    private volatile String charSet;

    // it will be initialized only if there are no dynamic csv formatting properties
    private volatile CSVFormat csvFormat;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(CSV_PARSER);
        properties.add(DateTimeUtils.DATE_FORMAT);
        properties.add(DateTimeUtils.TIME_FORMAT);
        properties.add(DateTimeUtils.TIMESTAMP_FORMAT);
        properties.add(CSVUtils.CSV_FORMAT);
        properties.add(CSVUtils.VALUE_SEPARATOR);
        properties.add(CSVUtils.RECORD_SEPARATOR);
        properties.add(CSVUtils.FIRST_LINE_IS_HEADER);
        properties.add(CSVUtils.IGNORE_CSV_HEADER);
        properties.add(CSVUtils.QUOTE_CHAR);
        properties.add(CSVUtils.ESCAPE_CHAR);
        properties.add(CSVUtils.COMMENT_MARKER);
        properties.add(CSVUtils.NULL_STRING);
        properties.add(CSVUtils.TRIM_FIELDS);
        properties.add(CSVUtils.CHARSET);
        properties.add(CSVUtils.ALLOW_DUPLICATE_HEADER_NAMES);
        properties.add(TRIM_DOUBLE_QUOTE);
        properties.add(FIELD_COUNT);
        properties.add(FAIL_ON_MISMATCH_FIELD_COUNT);
        properties.add(USE_SCHEMA_FOR_FIELD_COUNT);
        return properties;
    }

    @OnEnabled
    public void storeStaticProperties(final ConfigurationContext context) {
        this.context = context;

        this.csvParser = context.getProperty(CSV_PARSER).getValue();
        this.dateFormat = context.getProperty(DateTimeUtils.DATE_FORMAT).getValue();
        this.timeFormat = context.getProperty(DateTimeUtils.TIME_FORMAT).getValue();
        this.timestampFormat = context.getProperty(DateTimeUtils.TIMESTAMP_FORMAT).getValue();
        this.firstLineIsHeader = context.getProperty(CSVUtils.FIRST_LINE_IS_HEADER).asBoolean();
        this.ignoreHeader = context.getProperty(CSVUtils.IGNORE_CSV_HEADER).asBoolean();
        this.charSet = context.getProperty(CSVUtils.CHARSET).getValue();

        // Ensure that if we are deriving schema from header that we always treat the first line as a header,
        // regardless of the 'First Line is Header' property
        final String accessStrategy = context.getProperty(SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY).getValue();
        if (HEADER_DERIVED.getValue().equals(accessStrategy) || SchemaInferenceUtil.INFER_SCHEMA.getValue().equals(accessStrategy)) {
            this.firstLineIsHeader = true;
        }

        if (!CSVUtils.isDynamicCSVFormat(context)) {
            this.csvFormat = CSVUtils.createCSVFormat(context, Collections.emptyMap());
        } else {
            this.csvFormat = null;
        }
    }

    @Override
    public RecordReader createRecordReader(final Map<String, String> variables, final InputStream in, final long inputLength, final ComponentLog logger) throws IOException, SchemaNotFoundException {
        // Use Mark/Reset of a BufferedInputStream in case we read from the Input Stream for the header.
        in.mark(1024 * 1024);
        final RecordSchema schema = getSchema(variables, new NonCloseableInputStream(in), null);
        in.reset();

        final CSVFormat format;
        if (this.csvFormat != null) {
            format = this.csvFormat;
        } else {
            format = CSVUtils.createCSVFormat(context, variables);
        }

        final boolean trimDoubleQuote = context.getProperty(TRIM_DOUBLE_QUOTE).asBoolean();
        final Integer fieldCount = context.getProperty(FIELD_COUNT).asInteger();
        final boolean failOnMismatchFieldCount = context.getProperty(FAIL_ON_MISMATCH_FIELD_COUNT).asBoolean();
        final boolean useSchemaForFieldCount = context.getProperty(USE_SCHEMA_FOR_FIELD_COUNT).asBoolean();

        if (APACHE_COMMONS_CSV.getValue().equals(csvParser)) {
            return new CSVRecordReader(in, logger, schema, format, firstLineIsHeader, ignoreHeader, dateFormat, timeFormat, timestampFormat, charSet, trimDoubleQuote, useSchemaForFieldCount ? schema.getFieldCount() : fieldCount, failOnMismatchFieldCount);
        } else if (JACKSON_CSV.getValue().equals(csvParser)) {
            return new JacksonCSVRecordReader(in, logger, schema, format, firstLineIsHeader, ignoreHeader, dateFormat, timeFormat, timestampFormat, charSet, trimDoubleQuote);
        } else {
            throw new IOException("Parser not supported");
        }
    }

    @Override
    protected SchemaAccessStrategy getSchemaAccessStrategy(final String allowableValue, final SchemaRegistry schemaRegistry, final PropertyContext context) {
        if (allowableValue.equalsIgnoreCase(HEADER_DERIVED.getValue())) {
            return new CSVHeaderSchemaStrategy(context);
        } else if (allowableValue.equalsIgnoreCase(SchemaInferenceUtil.INFER_SCHEMA.getValue())) {
            final RecordSourceFactory<CSVRecordAndFieldNames> sourceFactory = (variables, in) -> new CSVRecordSource(in, context, variables);
            final SchemaInferenceEngine<CSVRecordAndFieldNames> inference = new CSVSchemaInference(new TimeValueInference(dateFormat, timeFormat, timestampFormat));
            return new InferSchemaAccessStrategy<>(sourceFactory, inference, getLogger());
        }

        return super.getSchemaAccessStrategy(allowableValue, schemaRegistry, context);
    }

    @Override
    protected List<AllowableValue> getSchemaAccessStrategyValues() {
        final List<AllowableValue> allowableValues = new ArrayList<>(super.getSchemaAccessStrategyValues());
        allowableValues.add(HEADER_DERIVED);
        allowableValues.add(SchemaInferenceUtil.INFER_SCHEMA);
        return allowableValues;
    }

    @Override
    protected AllowableValue getDefaultSchemaAccessStrategy() {
        return SchemaInferenceUtil.INFER_SCHEMA;
    }
}
