package io.datadynamics.nifi.db.bulkinsert;

import org.apache.kudu.shaded.com.google.common.base.Joiner;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.expression.ExpressionLanguageScope.FLOWFILE_ATTRIBUTES;
import static org.apache.nifi.expression.ExpressionLanguageScope.VARIABLE_REGISTRY;


@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"sql", "select", "jdbc", "query", "database"})
public class BulkOracleInsertProcessor extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully created FlowFile from SQL query result set.")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("SQL query execution failed. Incoming FlowFile will be penalized and routed to this relationship")
            .build();

    static final Relationship REL_RETRY = new Relationship.Builder()
            .name("retry")
            .description("A FlowFile is routed to this relationship if the database cannot be updated but attempting the operation again may succeed")
            .build();

    public static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("Database Connection Pooling Service")
            .description("The Controller Service that is used to obtain connection to database")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();

    static final PropertyDescriptor RECORD_READER_FACTORY = new PropertyDescriptor.Builder()
            .name("put-db-record-record-reader")
            .displayName("Record Reader")
            .description("입력 FlowFile에서 Record를 읽기 위한 Record Reader Service")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor BULK_INSERT_ROWS = new PropertyDescriptor.Builder()
            .name("bulk-insert-rows")
            .displayName("Bulk Insert Row Count")
            .description("벌크로 INSERT할 ROW 수")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("100")
            .required(true)
            .build();

    static final PropertyDescriptor QUERY_TIMEOUT = new PropertyDescriptor.Builder()
            .name("put-db-record-query-timeout")
            .displayName("Max Wait Time")
            .description("SQL문 실행시 허용하는 최대 시간(초). 0 설정시 무한대를 의미하며 1보다 작게 설정하는 경우 0과 동일하게 간주합니다.")
            .defaultValue("0 seconds")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor SCHEMA_NAME = new PropertyDescriptor.Builder()
            .name("put-db-record-schema-name")
            .displayName("Schema Name")
            .description("데이터를 넣을 테이블의 Schema명")
            .required(false)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("put-db-record-table-name")
            .displayName("Table Name")
            .description("데이터를 넣을 테이블명")
            .required(true)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    static final String PUT_DATABASE_RECORD_ERROR = "putdatabaserecord.error";

    protected DBCPService dbcpService;

    protected Set<Relationship> relationships;

    @Override
    public Set<Relationship> getRelationships() {
        relationships = new HashSet<>();
        relationships.add(REL_FAILURE);
        relationships.add(REL_SUCCESS);
        relationships.add(REL_RETRY);
        return relationships;
    }

    protected List<PropertyDescriptor> propDescriptors;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        propDescriptors = new ArrayList<>();
        propDescriptors.add(DBCP_SERVICE);
        propDescriptors.add(RECORD_READER_FACTORY);
        propDescriptors.add(BULK_INSERT_ROWS);
        propDescriptors.add(QUERY_TIMEOUT);
        propDescriptors.add(SCHEMA_NAME);
        propDescriptors.add(TABLE_NAME);
        return propDescriptors;
    }

    @OnScheduled
    public void setup(ProcessContext context) {
        this.dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        Optional<Connection> connectionHolder = Optional.empty();

        boolean originalAutoCommit = false;
        try {
            final Connection connection = dbcpService.getConnection(flowFile.getAttributes());
            connectionHolder = Optional.of(connection);

            originalAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);

            bulkInsert(context, session, flowFile, connection);
            connection.commit();

            session.transfer(flowFile, REL_SUCCESS);
            session.getProvenanceReporter().send(flowFile, getJdbcUrl(connection));
        } catch (final Exception e) {
            // 트랜잭션을 지원하므로 SQLTransientException 예외가 발생하는 경우 Retry를 시도한다.
            final Relationship relationship;
            final Throwable toAnalyze = (e instanceof BatchUpdateException) ? e.getCause() : e;
            if (toAnalyze instanceof SQLTransientException) {
                relationship = REL_RETRY;
                flowFile = session.penalize(flowFile);
            } else {
                relationship = REL_FAILURE;
            }

            getLogger().error("Failed to put Records to database for {}. Routing to {}.", flowFile, relationship, e);

            final boolean rollbackOnFailure = context.getProperty(RollbackOnFailure.ROLLBACK_ON_FAILURE).asBoolean();
            if (rollbackOnFailure) {
                session.rollback();
            } else {
                flowFile = session.putAttribute(flowFile, PUT_DATABASE_RECORD_ERROR, e.getMessage());
                session.transfer(flowFile, relationship);
            }

            connectionHolder.ifPresent(connection -> {
                try {
                    connection.rollback();
                } catch (final Exception rollbackException) {
                    getLogger().error("Failed to rollback JDBC transaction", rollbackException);
                }
            });
        } finally {
            if (originalAutoCommit) {
                connectionHolder.ifPresent(connection -> {
                    try {
                        connection.setAutoCommit(true);
                    } catch (final Exception autoCommitException) {
                        getLogger().warn("Failed to set auto-commit back to true on connection", autoCommitException);
                    }
                });
            }

            connectionHolder.ifPresent(connection -> {
                try {
                    connection.close();
                } catch (final Exception closeException) {
                    getLogger().warn("Failed to close database connection", closeException);
                }
            });
        }
    }

    private void bulkInsert(ProcessContext context, ProcessSession session, FlowFile flowFile, Connection connection) throws Exception {
        try (final InputStream in = session.read(flowFile)) {
            final RecordReaderFactory recordReaderFactory = context.getProperty(RECORD_READER_FACTORY).asControllerService(RecordReaderFactory.class);
            final RecordReader recordReader = recordReaderFactory.createRecordReader(flowFile, in, getLogger());
            _bulkInsert(context, flowFile, connection, recordReader);
        }
    }

    private void _bulkInsert(final ProcessContext context, final FlowFile flowFile, final Connection connection, final RecordReader recordReader)
            throws IllegalArgumentException, MalformedRecordException, IOException, SQLException {

        final String schemaName = context.getProperty(SCHEMA_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final Integer maxBulkRowCount = context.getProperty(BULK_INSERT_ROWS).evaluateAttributeExpressions().asInteger();

        final RecordSchema recordSchema = recordReader.getSchema();

        try (final Statement statement = connection.createStatement()) {
            final int timeoutMillis = context.getProperty(QUERY_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS).intValue();
            try {
                statement.setQueryTimeout(timeoutMillis); // 초단위 타임아웃
            } catch (SQLException se) {
                // 만약에 JDBC Driver가 Query Timeout을 지우너하지 않으면 무한으로 가정한다. 이 경우 0만 허용한다.
                if (timeoutMillis > 0) {
                    throw se;
                }
            }

            int count = 0;
            Record currentRecord;
            List<Record> records = new ArrayList();
            while ((currentRecord = recordReader.nextRecord()) != null) {
                if (count >= maxBulkRowCount) {
                    String sql = generateInsertQuery(schemaName, tableName, recordSchema, records);
                    statement.execute(sql);
                    records.clear();
                } else {
                    records.add(currentRecord);
                    count++;
                }
            }

            if (count > 0) {
                String sql = generateInsertQuery(schemaName, tableName, recordSchema, records);
                statement.execute(sql);
                records.clear();
            }
        }
    }

    private String generateInsertQuery(String schemaName, String tableName, RecordSchema recordSchema, List<Record> records) {
        StringBuilder builder = new StringBuilder();
        builder.append("INSERT ALL").append("\n");
        builder.append(_generateInsertQuery(schemaName, tableName, recordSchema, records)).append("\n");
        builder.append("SELECT 1 FROM dual;");
        return builder.toString();
    }

    private String _generateInsertQuery(String schemaName, String tableName, RecordSchema recordSchema, List<Record> records) {
        List<String> inserts = new ArrayList();
        for (Record record : records) {
            inserts.add(getInsertSQL(schemaName, tableName, recordSchema, record));
        }
        return Joiner.on("\n").join(inserts);
    }

    private String getInsertSQL(String schemaName, String tableName, RecordSchema recordSchema, Record record) {
        return String.format("INTO %s ( %s ) VALUES ( %s )", getTableName(schemaName, tableName), getColumns(recordSchema), getValues(recordSchema, record));
    }

    private String getTableName(String schemaName, String tableName) {
        return String.format("%s.%s", schemaName, tableName);
    }

    private String getColumns(RecordSchema recordSchema) {
        List<RecordField> fields = recordSchema.getFields();
        List<String> columns = new ArrayList<>();
        for (RecordField field : fields) {
            columns.add(field.getFieldName());
        }
        return Joiner.on(", ").join(columns);
    }

    private String getValues(RecordSchema recordSchema, Record record) {
        List<String> values = new ArrayList<>();
        List<RecordField> fields = recordSchema.getFields();
        for (RecordField field : fields) {
            values.add(getValue(field, record));
        }
        return Joiner.on(", ").join(values);
    }

    private String getValue(RecordField field, Record record) {
        switch (field.getDataType().getFieldType()) {
            case DATE:
            case TIME:
            case TIMESTAMP:
                return String.format("'%s'", String.valueOf(record.getValue(field.getFieldName())));
            case BIGINT:
            case BYTE:
            case DECIMAL:
            case FLOAT:
            case SHORT:
            case LONG:
            case INT:
            case DOUBLE:
            case BOOLEAN:
                return String.valueOf(record.getValue(field.getFieldName()));
            case CHAR:
            case STRING:
            default:
                return String.format("'%s'", record.getValue(field.getFieldName()));
        }
    }

    private String getJdbcUrl(final Connection connection) {
        try {
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            if (databaseMetaData != null) {
                return databaseMetaData.getURL();
            }
        } catch (final Exception e) {
            getLogger().warn("Could not determine JDBC URL based on the Driver Connection.", e);
        }

        return "DBCPService";
    }
}