package io.datadynamics.nifi.db.bulkinsert;


import io.datadynamics.nifi.db.executesql.ExecuteSQL;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestBulkOracleInsertProcessor {

    private final Logger LOGGER = LoggerFactory.getLogger(TestBulkOracleInsertProcessor.class);

    @BeforeAll
    public static void setupClass() {
        System.setProperty("derby.stream.error.file", "target/derby.log");
    }

    @AfterAll
    public static void cleanupClass() {
        System.clearProperty("derby.stream.error.file");
    }

    private TestRunner runner;


    private MockRecordParser parser;

    @BeforeEach
    public void setup() throws InitializationException {
        final DBCPService dbcp = new DBCPServiceSimpleImpl();
        final Map<String, String> dbcpProperties = new HashMap<>();

        parser = new MockRecordParser();
        parser.addSchemaField("TypeBoolean", RecordFieldType.BOOLEAN);
        parser.addSchemaField("TypeInt", RecordFieldType.INT);
        parser.addSchemaField("TypeLong", RecordFieldType.LONG);
        parser.addSchemaField("TypeFloat", RecordFieldType.FLOAT);
        parser.addSchemaField("TypeDouble", RecordFieldType.DOUBLE);
        parser.addSchemaField("TypeString", RecordFieldType.STRING);
        parser.addSchemaField("TypeBytesDecimal", RecordFieldType.BYTE);
        parser.addSchemaField("TypeDate", RecordFieldType.INT);
        parser.addSchemaField("TypeTimeInMillis", RecordFieldType.INT);
        parser.addSchemaField("TypeTimeInMicros", RecordFieldType.LONG);
        parser.addSchemaField("TypeTimestampInMillis", RecordFieldType.LONG);
        parser.addSchemaField("TypeStringTimestampInMillis", RecordFieldType.STRING);
        parser.enabled();

        runner = TestRunners.newTestRunner(BulkOracleInsertProcessor.class);
        runner.addControllerService("dbcp", dbcp, dbcpProperties);
        runner.enableControllerService(dbcp);
        runner.addControllerService("reader", parser, dbcpProperties);
        runner.enableControllerService(parser);

        runner.setProperty(BulkOracleInsertProcessor.DBCP_SERVICE, "dbcp");
        runner.setProperty(BulkOracleInsertProcessor.TABLE_NAME, "table1");
        runner.setProperty(BulkOracleInsertProcessor.SCHEMA_NAME, "schema1");
        runner.setProperty(BulkOracleInsertProcessor.BULK_INSERT_ROWS, "10");
        runner.setProperty(BulkOracleInsertProcessor.RECORD_READER_FACTORY, "reader");
        runner.setProperty(RollbackOnFailure.ROLLBACK_ON_FAILURE, "true");
        runner.setProperty(BulkOracleInsertProcessor.AVRO_SCHEMA, "{\n" +
                "  \"namespace\": \"io.datadynamics.template.avro.model.types2\",\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"DataTypes2\",\n" +
                "  \"fields\": [\n" +
                "    {\n" +
                "      \"name\": \"TypeBoolean\",\n" +
                "      \"type\": [\"null\", \"boolean\"]\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"TypeInt\",\n" +
                "      \"type\": [\"null\", \"int\"]\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"TypeLong\",\n" +
                "      \"type\": [\"null\", \"long\"]\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"TypeFloat\",\n" +
                "      \"type\": [\"null\", \"float\"]\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"TypeDouble\",\n" +
                "      \"type\": [\"null\", \"double\"]\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"TypeString\",\n" +
                "      \"type\": [\"null\", \"string\"]\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"TypeBytesDecimal\",\n" +
                "      \"type\": [\"null\", {\"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": 6, \"scale\": 2}]\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"TypeDate\",\n" +
                "      \"type\": [\"null\", {\"type\": \"int\", \"logicalType\": \"date\"}]\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"TypeTimeInMillis\",\n" +
                "      \"type\": [\"null\", {\"type\": \"int\", \"logicalType\": \"time-millis\"}]\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"TypeTimeInMicros\",\n" +
                "      \"type\": [\"null\", {\"type\": \"long\", \"logicalType\": \"time-micros\"}]\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"TypeTimestampInMillis\",\n" +
                "      \"type\": [\"null\", {\"type\": \"long\", \"logicalType\": \"timestamp-millis\"}]\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"TypeStringTimestampInMillis\",\n" +
                "      \"type\": [\"null\", {\"type\": \"string\", \"logicalType\": \"timestamp-millis\"}]\n" +
                "    }\n" +
                "  ]\n" +
                "}");
    }

    @Test
    public void test() throws Exception {
        DatumReader datumReader = new SpecificDatumReader<>();
        DataFileReader dataFileReader = new DataFileReader<>(new File("src/test/resources/sample.avro"), datumReader);
        List<GenericData.Record> records = new ArrayList();
        while (dataFileReader.hasNext()) {
            GenericData.Record record = (GenericData.Record) dataFileReader.next();
            parser.addRecord(record);
        }

        MockFlowFile mff = new MockFlowFile(System.currentTimeMillis());
        mff.setData(this.getClass().getClassLoader().getResourceAsStream("sample.avro").readAllBytes());
        runner.enqueue(mff);
        runner.run();
//        runner.assertTransferCount(ExecuteSQL.REL_SUCCESS, 1);
//        runner.assertTransferCount(ExecuteSQL.REL_FAILURE, 0);
    }

    /**
     * Simple implementation only for ExecuteSQL processor testing.
     */
    class DBCPServiceSimpleImpl extends AbstractControllerService implements DBCPService {

        @Override
        public String getIdentifier() {
            return "dbcp";
        }

        @Override
        public Connection getConnection() throws ProcessException {
            try {
                Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
                final Connection con = DriverManager.getConnection("jdbc:derby:db;create=true");
                return con;
            } catch (final Exception e) {
                throw new ProcessException("getConnection failed: " + e);
            }
        }
    }

}