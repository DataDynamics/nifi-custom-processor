package io.datadynamics.nifi.record.parquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processors.hadoop.AbstractPutHDFSRecord;
import org.apache.nifi.processors.hadoop.record.HDFSRecordWriter;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"put", "parquet", "hadoop", "HDFS", "filesystem", "record"})
@CapabilityDescription("Reads records from an incoming FlowFile using the provided Record Reader, and writes those records " +
        "to a Parquet file. The schema for the Parquet file must be provided in the processor properties. This processor will " +
        "first write a temporary dot file and upon successfully writing every record to the dot file, it will rename the " +
        "dot file to it's final name. If the dot file cannot be renamed, the rename operation will be attempted up to 10 times, and " +
        "if still not successful, the dot file will be deleted and the flow file will be routed to failure. " +
        " If any error occurs while reading records from the input, or writing records to the output, " +
        "the entire dot file will be removed and the flow file will be routed to failure or retry, depending on the error.")
@ReadsAttribute(attribute = "filename", description = "The name of the file to write comes from the value of this attribute.")
@WritesAttributes({
        @WritesAttribute(attribute = "filename", description = "The name of the file is stored in this attribute."),
        @WritesAttribute(attribute = "absolute.hdfs.path", description = "The absolute path to the file is stored in this attribute."),
        @WritesAttribute(attribute = "hadoop.file.url", description = "The hadoop url for the file is stored in this attribute."),
        @WritesAttribute(attribute = "record.count", description = "The number of records written to the Parquet file")
})
@Restricted(restrictions = {
        @Restriction(
                requiredPermission = RequiredPermission.WRITE_DISTRIBUTED_FILESYSTEM,
                explanation = "Provides operator the ability to write any file that NiFi has access to in HDFS or the local filesystem.")
})
public class PutParquet extends AbstractPutHDFSRecord {

    public static final PropertyDescriptor REMOVE_CRC_FILES = new PropertyDescriptor.Builder()
            .name("remove-crc-files")
            .displayName("Remove CRC Files")
            .description("Specifies whether the corresponding CRC file should be deleted upon successfully writing a Parquet file")
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    @Override
    public List<AllowableValue> getCompressionTypes(final ProcessorInitializationContext context) {
        return ParquetUtils.COMPRESSION_TYPES;
    }

    @Override
    public String getDefaultCompressionType(final ProcessorInitializationContext context) {
        return CompressionCodecName.UNCOMPRESSED.name();
    }

    @Override
    public List<PropertyDescriptor> getAdditionalProperties() {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(ParquetUtils.ROW_GROUP_SIZE);
        props.add(ParquetUtils.PAGE_SIZE);
        props.add(ParquetUtils.DICTIONARY_PAGE_SIZE);
        props.add(ParquetUtils.MAX_PADDING_SIZE);
        props.add(ParquetUtils.ENABLE_DICTIONARY_ENCODING);
        props.add(ParquetUtils.ENABLE_VALIDATION);
        props.add(ParquetUtils.WRITER_VERSION);
        props.add(ParquetUtils.AVRO_WRITE_OLD_LIST_STRUCTURE);
        props.add(ParquetUtils.AVRO_ADD_LIST_ELEMENT_RECORDS);
        props.add(REMOVE_CRC_FILES);
        return Collections.unmodifiableList(props);
    }

    @Override
    public HDFSRecordWriter createHDFSRecordWriter(final ProcessContext context, final FlowFile flowFile, final Configuration conf, final Path path, final RecordSchema schema)
            throws IOException, SchemaNotFoundException {

        final Schema avroSchema = AvroTypeUtil.extractAvroSchema(schema);

        final AvroParquetWriter.Builder<GenericRecord> parquetWriter = AvroParquetWriter
                .<GenericRecord>builder(path)
                .withSchema(avroSchema);

        final ParquetConfig parquetConfig = ParquetUtils.createParquetConfig(context, flowFile.getAttributes());
        ParquetUtils.applyCommonConfig(parquetWriter, conf, parquetConfig);

        return new AvroParquetHDFSRecordWriter(parquetWriter.build(), avroSchema);
    }

    @Override
    protected FlowFile postProcess(final ProcessContext context, final ProcessSession session, final FlowFile flowFile, final Path destFile) {
        final boolean removeCRCFiles = context.getProperty(REMOVE_CRC_FILES).asBoolean();
        if (removeCRCFiles) {
            final String filename = destFile.getName();
            final String hdfsPath = destFile.getParent().toString();

            final Path crcFile = new Path(hdfsPath, "." + filename + ".crc");
            deleteQuietly(getFileSystem(), crcFile);
        }

        return flowFile;
    }
}
