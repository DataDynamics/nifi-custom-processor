package io.datadynamics.nifi.record.avro;

import io.datadynamics.nifi.record.parquet.AvroTypeUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaAccessWriter;
import org.apache.nifi.serialization.AbstractRecordSetWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class WriteAvroResultWithExternalSchema extends AbstractRecordSetWriter {
    private final SchemaAccessWriter schemaAccessWriter;
    private final RecordSchema recordSchema;
    private final Schema avroSchema;
    private final BinaryEncoder encoder;
    private final OutputStream buffered;
    private final DatumWriter<GenericRecord> datumWriter;
    private final BlockingQueue<BinaryEncoder> recycleQueue;
    private final String timestampFormatPropertyKeyName;
    private final int addHours;
    private boolean closed = false;

    public WriteAvroResultWithExternalSchema(final Schema avroSchema, final RecordSchema recordSchema, final SchemaAccessWriter schemaAccessWriter,
                                             final OutputStream out, final BlockingQueue<BinaryEncoder> recycleQueue, final ComponentLog logger, String timestampFormatPropertyKeyName, int addHours) {
        super(out);
        this.recordSchema = recordSchema;
        this.schemaAccessWriter = schemaAccessWriter;
        this.avroSchema = avroSchema;
        this.buffered = new BufferedOutputStream(out);
        this.recycleQueue = recycleQueue;
        this.timestampFormatPropertyKeyName = timestampFormatPropertyKeyName;
        this.addHours = addHours;

        BinaryEncoder reusableEncoder = recycleQueue.poll();
        if (reusableEncoder == null) {
            logger.debug("Was not able to obtain a BinaryEncoder from reuse pool. This is normal for the first X number of iterations (where X is equal to the max size of the pool), " +
                    "but if this continues, it indicates that increasing the size of the pool will likely yield better performance for this Avro Writer.");
        }

        encoder = EncoderFactory.get().blockingBinaryEncoder(buffered, reusableEncoder);

        datumWriter = new GenericDatumWriter<>(avroSchema);
    }

    @Override
    protected void onBeginRecordSet() throws IOException {
        schemaAccessWriter.writeHeader(recordSchema, buffered);
    }

    @Override
    protected Map<String, String> onFinishRecordSet() throws IOException {
        flush();
        return schemaAccessWriter.getAttributes(recordSchema);
    }

    @Override
    public Map<String, String> writeRecord(final Record record) throws IOException {
        // If we are not writing an active record set, then we need to ensure that we write the
        // schema information.
        if (!isActiveRecordSet()) {
            flush();
            schemaAccessWriter.writeHeader(recordSchema, getOutputStream());
        }

        final GenericRecord rec = AvroTypeUtil.createAvroRecord(record, avroSchema, timestampFormatPropertyKeyName, addHours);
        datumWriter.write(rec, encoder);
        return schemaAccessWriter.getAttributes(recordSchema);
    }

    @Override
    public void flush() throws IOException {
        encoder.flush();
        buffered.flush();
    }

    @Override
    public String getMimeType() {
        return "application/avro-binary";
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;

        if (encoder != null) {
            flush();
            recycleQueue.offer(encoder);
        }

        super.close();
    }
}
