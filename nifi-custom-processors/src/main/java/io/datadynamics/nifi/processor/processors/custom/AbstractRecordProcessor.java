package io.datadynamics.nifi.processor.processors.custom;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.*;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractRecordProcessor extends AbstractProcessor {

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("Specifies the Controller Service to use for reading incoming data")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record Writer")
            .description("Specifies the Controller Service to use for writing out the records")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor INCLUDE_ZERO_RECORD_FLOWFILES = new PropertyDescriptor.Builder()
            .name("include-zero-record-flowfiles")
            .displayName("Include Zero Record FlowFiles")
            .description("When converting an incoming FlowFile, if the conversion results in no data, "
                    + "this property specifies whether or not a FlowFile will be sent to the corresponding relationship")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that are successfully transformed will be routed to this relationship")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If a FlowFile cannot be transformed from the configured input format to the configured output format, "
                    + "the unchanged FlowFile will be routed to this relationship")
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(RECORD_READER);
        properties.add(RECORD_WRITER);
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        return relationships;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        final boolean includeZeroRecordFlowFiles = context.getProperty(INCLUDE_ZERO_RECORD_FLOWFILES).isSet() ? context.getProperty(INCLUDE_ZERO_RECORD_FLOWFILES).asBoolean() : true;

        final Map<String, String> attributes = new HashMap<>();
        final AtomicInteger recordCount = new AtomicInteger();

        final FlowFile original = flowFile;
        final Map<String, String> originalAttributes = flowFile.getAttributes();
        try {
            flowFile = session.write(flowFile, new StreamCallback() {
                @Override
                public void process(final InputStream in, final OutputStream out) throws IOException {

                    try (final RecordReader reader = readerFactory.createRecordReader(originalAttributes, in, original.getSize(), getLogger())) {

                        // Get the first record and process it before we create the Record Writer. We do this so that if the Processor
                        // updates the Record's schema, we can provide an updated schema to the Record Writer. If there are no records,
                        // then we can simply create the Writer with the Reader's schema and begin & end the Record Set.
                        Record firstRecord = reader.nextRecord();
                        if (firstRecord == null) {
                            final RecordSchema writeSchema = writerFactory.getSchema(originalAttributes, reader.getSchema());
                            try (final RecordSetWriter writer = writerFactory.createWriter(getLogger(), writeSchema, out, originalAttributes)) {
                                writer.beginRecordSet();

                                final WriteResult writeResult = writer.finishRecordSet();
                                attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                                attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                                attributes.putAll(writeResult.getAttributes());
                                recordCount.set(writeResult.getRecordCount());
                            }

                            return;
                        }

                        firstRecord = AbstractRecordProcessor.this.process(firstRecord, original, context, 1L);

                        final RecordSchema writeSchema = writerFactory.getSchema(originalAttributes, firstRecord.getSchema());
                        try (final RecordSetWriter writer = writerFactory.createWriter(getLogger(), writeSchema, out, originalAttributes)) {
                            writer.beginRecordSet();

                            writer.write(firstRecord);

                            Record record;
                            long count = 1L;
                            while ((record = reader.nextRecord()) != null) {
                                final Record processed = AbstractRecordProcessor.this.process(record, original, context, ++count);
                                writer.write(processed);
                            }

                            final WriteResult writeResult = writer.finishRecordSet();
                            attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                            attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                            attributes.putAll(writeResult.getAttributes());
                            recordCount.set(writeResult.getRecordCount());
                        }
                    } catch (final SchemaNotFoundException e) {
                        throw new ProcessException(e.getLocalizedMessage(), e);
                    } catch (final MalformedRecordException e) {
                        throw new ProcessException("Could not parse incoming data", e);
                    }
                }
            });
        } catch (final Exception e) {
            getLogger().error("Failed to process {}; will route to failure", new Object[]{flowFile, e});
            // Since we are wrapping the exceptions above there should always be a cause
            // but it's possible it might not have a message. This handles that by logging
            // the name of the class thrown.
            Throwable c = e.getCause();
            if (c != null) {
                session.putAttribute(flowFile, "record.error.message", (c.getLocalizedMessage() != null) ? c.getLocalizedMessage() : c.getClass().getCanonicalName() + " Thrown");
            } else {
                session.putAttribute(flowFile, "record.error.message", e.getClass().getCanonicalName() + " Thrown");
            }
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        flowFile = session.putAllAttributes(flowFile, attributes);
        if (!includeZeroRecordFlowFiles && recordCount.get() == 0) {
            session.remove(flowFile);
        } else {
            session.transfer(flowFile, REL_SUCCESS);
        }

        final int count = recordCount.get();
        session.adjustCounter("Records Processed", count, false);
        getLogger().info("Successfully converted {} records for {}", new Object[]{count, flowFile});
    }

    protected abstract Record process(Record record, FlowFile flowFile, ProcessContext context, long count);
}