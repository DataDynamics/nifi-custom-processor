package io.datadynamics.nifi.record.parquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaAccessWriter;
import org.apache.nifi.serialization.AbstractRecordSetWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.OutputFile;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Map;

public class WriteParquetResult extends AbstractRecordSetWriter {

    private final Schema schema;
    private final ParquetWriter<GenericRecord> parquetWriter;
    private final ComponentLog componentLogger;
    private SchemaAccessWriter accessWriter;
    private RecordSchema recordSchema;

    private final String timestampFormatPropertyKeyName; // FIXED
    private int addHours; // FIXED

    public WriteParquetResult(final Schema avroSchema, final RecordSchema recordSchema, final SchemaAccessWriter accessWriter, final OutputStream out,
                              final ParquetConfig parquetConfig, final ComponentLog componentLogger, String timestampFormatPropertyKeyName, int addHours) throws IOException { // FIXED
        super(out);
        this.schema = avroSchema;
        this.componentLogger = componentLogger;
        this.accessWriter = accessWriter;
        this.recordSchema = recordSchema;
        this.timestampFormatPropertyKeyName = timestampFormatPropertyKeyName; // FIXED
        this.addHours = addHours; // FIXED

        final Configuration conf = new Configuration();
        final OutputFile outputFile = new NifiParquetOutputFile(out);

        final AvroParquetWriter.Builder<GenericRecord> writerBuilder = AvroParquetWriter.<GenericRecord>builder(outputFile).withSchema(avroSchema);
        ParquetUtils.applyCommonConfig(writerBuilder, conf, parquetConfig);
        parquetWriter = writerBuilder.build();

        if (componentLogger.isInfoEnabled()) { // FIXED
            componentLogger.info("[DFM] WriteParquetResult : Schema = {}", schema);
            componentLogger.info("[DFM] WriteParquetResult : timestampFormatPropertyKeyName = {}, addHours = {}", this.timestampFormatPropertyKeyName, this.addHours);
        }
    }

    @Override
    protected Map<String, String> writeRecord(final Record record) throws IOException {
        if (componentLogger.isInfoEnabled()) {
            this.componentLogger.info("[DFM] [Write] Record = {}", record);
        }

        final GenericRecord genericRecord = AvroTypeUtil.createAvroRecord(record, schema, timestampFormatPropertyKeyName, addHours);
        return Collections.emptyMap();
    }

    @Override
    protected Map<String, String> onFinishRecordSet() {
        return accessWriter.getAttributes(recordSchema);
    }

    @Override
    public void close() throws IOException {
        try {
            parquetWriter.close();
        } finally {
            // ensure the output stream still gets closed
            super.close();
        }
    }

    @Override
    public String getMimeType() {
        return "application/parquet";
    }

}

