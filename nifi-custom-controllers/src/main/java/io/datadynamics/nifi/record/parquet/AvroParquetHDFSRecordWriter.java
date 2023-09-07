package io.datadynamics.nifi.record.parquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.processors.hadoop.record.HDFSRecordWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.parquet.hadoop.ParquetWriter;

import java.io.IOException;

/**
 * HDFSRecordWriter that writes Parquet files using Avro as the schema representation.
 */
public class AvroParquetHDFSRecordWriter implements HDFSRecordWriter {

    private final Schema avroSchema;
    private final ParquetWriter<GenericRecord> parquetWriter;

    public AvroParquetHDFSRecordWriter(final ParquetWriter<GenericRecord> parquetWriter, final Schema avroSchema) {
        this.avroSchema = avroSchema;
        this.parquetWriter = parquetWriter;
    }

    @Override
    public void write(final Record record) throws IOException {
        final GenericRecord genericRecord = AvroTypeUtil.createAvroRecord(record, avroSchema);
        parquetWriter.write(genericRecord);
    }

    @Override
    public void close() throws IOException {
        parquetWriter.close();
    }

}