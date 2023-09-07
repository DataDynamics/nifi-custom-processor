package io.datadynamics.nifi.record.avro;

import io.datadynamics.nifi.record.parquet.AvroTypeUtil;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.nifi.serialization.AbstractRecordSetWriter;
import org.apache.nifi.serialization.record.Record;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Map;

public class WriteAvroResultWithSchema extends AbstractRecordSetWriter {

    private final DataFileWriter<GenericRecord> dataFileWriter;
    private final Schema schema;
    private final String timestampFormatPropertyKeyName;
    private final int addHours;

    public WriteAvroResultWithSchema(final Schema schema, final OutputStream out, final CodecFactory codec, String timestampFormatPropertyKeyName, int addHours) throws IOException {
        super(out);
        this.schema = schema;
        this.timestampFormatPropertyKeyName = timestampFormatPropertyKeyName;
        this.addHours = addHours;

        final GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.setCodec(codec);
        dataFileWriter.create(schema, out);
    }

    @Override
    public void close() throws IOException {
        dataFileWriter.close();
    }

    @Override
    public void flush() throws IOException {
        dataFileWriter.flush();
    }

    @Override
    public Map<String, String> writeRecord(final Record record) throws IOException {
        final GenericRecord rec = AvroTypeUtil.createAvroRecord(record, schema, timestampFormatPropertyKeyName, addHours);
        dataFileWriter.append(rec);
        return Collections.emptyMap();
    }

    @Override
    public String getMimeType() {
        return "application/avro-binary";
    }
}
