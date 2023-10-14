package io.datadynamics.nifi.kudu;

import io.datadynamics.nifi.kudu.converter.FieldConverter;
import io.datadynamics.nifi.kudu.converter.ObjectTimestampFieldConverter;
import io.datadynamics.nifi.kudu.json.TimestampFormatHolder;
import io.datadynamics.nifi.kudu.util.DataTypeUtils;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnTypeAttributes;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.kerberos.KerberosCredentialsService;
import org.apache.nifi.kerberos.KerberosUserService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.security.krb.KerberosAction;
import org.apache.nifi.security.krb.KerberosKeytabUser;
import org.apache.nifi.security.krb.KerberosPasswordUser;
import org.apache.nifi.security.krb.KerberosUser;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.type.DecimalDataType;
import org.apache.nifi.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

public abstract class AbstractKuduProcessor extends AbstractProcessor {

    static final PropertyDescriptor KUDU_MASTERS = new Builder()
            .name("kudu-masters")
            .name("Kudu Masters")
            .description("연결할 Kudu Master 주소이며 쉼표로 나열할 수 있습니다.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    static final PropertyDescriptor KERBEROS_CREDENTIALS_SERVICE = new Builder()
            .name("kerberos-credentials-service")
            .displayName("Kerberos Credentials Service")
            .description("Kerberos 인증시 필요한 Credential Service를 지정하십시오.")
            .required(false)
            .identifiesControllerService(KerberosCredentialsService.class)
            .build();
    static final PropertyDescriptor KERBEROS_USER_SERVICE = new PropertyDescriptor.Builder()
            .name("kerberos-user-service")
            .displayName("Kerberos User Service")
            .description("Kerberos 인증시 필요한 Kerberos User Controller Service를 지정하십시오.")
            .identifiesControllerService(KerberosUserService.class)
            .required(false)
            .build();
    static final PropertyDescriptor KERBEROS_PRINCIPAL = new PropertyDescriptor.Builder()
            .name("kerberos-principal")
            .displayName("Kerberos Principal")
            .description("Kerberos 인증시 ")
            .description("The principal to use when specifying the principal and password " + "directly in the processor for authenticating via Kerberos.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    static final PropertyDescriptor KERBEROS_PASSWORD = new PropertyDescriptor.Builder()
            .name("kerberos-password")
            .displayName("Kerberos Password")
            .description("The password to use when specifying the principal and password directly in the processor for authenticating via Kerberos.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();
    static final PropertyDescriptor KUDU_OPERATION_TIMEOUT_MS = new Builder()
            .name("kudu-operations-timeout-ms")
            .displayName("Kudu Operation Timeout")
            .description("Default timeout used for user operations (using sessions and scanners)")
            .required(false)
            .defaultValue(AsyncKuduClient.DEFAULT_OPERATION_TIMEOUT_MS + "ms")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    static final PropertyDescriptor KUDU_SASL_PROTOCOL_NAME = new Builder()
            .name("kudu-sasl-protocol-name")
            .displayName("Kudu SASL 프로토콜명")
            .description("Kerberos를 통해 인증시 사용할 프로토콜명. Service Principal 명과 반드시 일치해야 합니다.")
            .required(false)
            .defaultValue("kudu")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    /**
     * 기본 Kudu Client의 Worker Thread는 Core의 개수만큼 설정이 되므로 baremetal에서는 이 값을 낮춰서 설정해야 한다.
     */
    private static final int DEFAULT_WORKER_COUNT = 4;
    static final PropertyDescriptor WORKER_COUNT = new Builder().name("worker-count").displayName("Kudu Client Worker 쓰레드 개수").description("Kudu 클라이언트 읽기 및 쓰기 작업을 처리하는 최대 작업자 스레드 수입니다. 기본적으로 사용 가능한 프로세서의 개수 입니다. 현재 사용가능한 프로세스의 개수: " + Runtime.getRuntime().availableProcessors()).required(true).defaultValue(Integer.toString(DEFAULT_WORKER_COUNT)).addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR).build();
    private static final FieldConverter<Object, Timestamp> TIMESTAMP_FIELD_CONVERTER = new ObjectTimestampFieldConverter();
    /**
     * Timestamp Pattern overrides default RecordFieldType.TIMESTAMP pattern of yyyy-MM-dd HH:mm:ss with optional microseconds
     */
    private static final String MICROSECOND_TIMESTAMP_PATTERN = "yyyy-MM-dd HH:mm:ss[.SSSSSS]";
    private final ReadWriteLock kuduClientReadWriteLock = new ReentrantReadWriteLock();
    private final Lock kuduClientReadLock = kuduClientReadWriteLock.readLock();
    private final Lock kuduClientWriteLock = kuduClientReadWriteLock.writeLock();
    Logger logger = LoggerFactory.getLogger(AbstractProcessor.class);
    private volatile KuduClient kuduClient;

    private volatile KerberosUser kerberosUser;

    protected KerberosUser getKerberosUser() {
        return this.kerberosUser;
    }

    protected boolean supportsIgnoreOperations() {
        try {
            return kuduClient.supportsIgnoreOperations();
        } catch (KuduException e) {
            throw new RuntimeException(e);
        }
    }

    protected void createKerberosUserAndOrKuduClient(ProcessContext context) {
        final KerberosUserService kerberosUserService = context.getProperty(KERBEROS_USER_SERVICE).asControllerService(KerberosUserService.class);
        if (kerberosUserService != null) {
            kerberosUser = kerberosUserService.createKerberosUser();
            kerberosUser.login();
            createKuduClient(context);
        } else {
            final KerberosCredentialsService credentialsService = context.getProperty(KERBEROS_CREDENTIALS_SERVICE).asControllerService(KerberosCredentialsService.class);
            final String kerberosPrincipal = context.getProperty(KERBEROS_PRINCIPAL).evaluateAttributeExpressions().getValue();
            final String kerberosPassword = context.getProperty(KERBEROS_PASSWORD).getValue();

            if (credentialsService != null) {
                kerberosUser = createKerberosKeytabUser(credentialsService.getPrincipal(), credentialsService.getKeytab(), context);
                kerberosUser.login(); // login creates the kudu client as well
            } else if (!StringUtils.isBlank(kerberosPrincipal) && !StringUtils.isBlank(kerberosPassword)) {
                kerberosUser = createKerberosPasswordUser(kerberosPrincipal, kerberosPassword, context);
                kerberosUser.login(); // login creates the kudu client as well
            } else {
                createKuduClient(context);
            }
        }
    }

    protected void createKuduClient(ProcessContext context) {
        kuduClientWriteLock.lock();
        try {
            if (this.kuduClient != null) {
                try {
                    this.kuduClient.close();
                } catch (KuduException e) {
                    getLogger().error("Kudu 클라이언트를 종료할 수 없습니다.");
                }
            }

            if (kerberosUser != null) {
                final KerberosAction<KuduClient> kerberosAction = new KerberosAction<>(kerberosUser, () -> buildClient(context), getLogger());
                this.kuduClient = kerberosAction.execute();
            } else {
                this.kuduClient = buildClient(context);
            }
        } finally {
            kuduClientWriteLock.unlock();
        }
    }

    protected KuduClient buildClient(final ProcessContext context) {
        final String masters = context.getProperty(KUDU_MASTERS).evaluateAttributeExpressions().getValue();
        final int operationTimeout = context.getProperty(KUDU_OPERATION_TIMEOUT_MS).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS).intValue();
        final String saslProtocolName = context.getProperty(KUDU_SASL_PROTOCOL_NAME).evaluateAttributeExpressions().getValue();
        final int workerCount = context.getProperty(WORKER_COUNT).asInteger();

        // Create Executor following approach of Executors.newCachedThreadPool() using worker count as maximum pool size
        final int corePoolSize = 0;
        final long threadKeepAliveTime = 60;
        final Executor nioExecutor = new ThreadPoolExecutor(corePoolSize, workerCount, threadKeepAliveTime, TimeUnit.SECONDS, new SynchronousQueue<>(), new ClientThreadFactory(getIdentifier()));

        return new KuduClient.KuduClientBuilder(masters).defaultOperationTimeoutMs(operationTimeout).saslProtocolName(saslProtocolName).workerCount(workerCount).nioExecutor(nioExecutor).build();
    }

    protected void executeOnKuduClient(Consumer<KuduClient> actionOnKuduClient) {
        kuduClientReadLock.lock();
        try {
            actionOnKuduClient.accept(kuduClient);
        } finally {
            kuduClientReadLock.unlock();
        }
    }

    protected void flushKuduSession(final KuduSession kuduSession, boolean close, final List<RowError> rowErrors) throws KuduException {
        final List<OperationResponse> responses = close ? kuduSession.close() : kuduSession.flush();

        if (kuduSession.getFlushMode() == SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND) {
            rowErrors.addAll(Arrays.asList(kuduSession.getPendingErrors().getRowErrors()));
        } else {
            responses.stream().filter(OperationResponse::hasRowError).map(OperationResponse::getRowError).forEach(rowErrors::add);
        }
    }

    protected KerberosUser createKerberosKeytabUser(String principal, String keytab, ProcessContext context) {
        return new KerberosKeytabUser(principal, keytab) {
            @Override
            public synchronized void login() {
                if (isLoggedIn()) {
                    return;
                }

                super.login();
                createKuduClient(context);
            }
        };
    }

    protected KerberosUser createKerberosPasswordUser(String principal, String password, ProcessContext context) {
        return new KerberosPasswordUser(principal, password) {
            @Override
            public synchronized void login() {
                if (isLoggedIn()) {
                    return;
                }

                super.login();
                createKuduClient(context);
            }
        };
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        final List<ValidationResult> results = new ArrayList<>();

        final boolean kerberosPrincipalProvided = !StringUtils.isBlank(context.getProperty(KERBEROS_PRINCIPAL).evaluateAttributeExpressions().getValue());
        final boolean kerberosPasswordProvided = !StringUtils.isBlank(context.getProperty(KERBEROS_PASSWORD).getValue());

        if (kerberosPrincipalProvided && !kerberosPasswordProvided) {
            results.add(new ValidationResult.Builder().subject(KERBEROS_PASSWORD.getDisplayName()).valid(false).explanation("지정한 Principal을 위한 Password가 필요합니다").build());
        }

        if (kerberosPasswordProvided && !kerberosPrincipalProvided) {
            results.add(new ValidationResult.Builder().subject(KERBEROS_PRINCIPAL.getDisplayName()).valid(false).explanation("지정한 Passowrd를 위한 Principal이 필요합니다.").build());
        }

        final KerberosCredentialsService kerberosCredentialsService = context.getProperty(KERBEROS_CREDENTIALS_SERVICE).asControllerService(KerberosCredentialsService.class);
        final KerberosUserService kerberosUserService = context.getProperty(KERBEROS_USER_SERVICE).asControllerService(KerberosUserService.class);

        if (kerberosCredentialsService != null && (kerberosPrincipalProvided || kerberosPasswordProvided)) {
            results.add(new ValidationResult.Builder().subject(KERBEROS_CREDENTIALS_SERVICE.getDisplayName()).valid(false).explanation("kerberos Principal/Password와 Kerberos Credential Service는 동시에 설정할 수 없습니다.").build());
        }

        if (kerberosUserService != null && (kerberosPrincipalProvided || kerberosPasswordProvided)) {
            results.add(new ValidationResult.Builder().subject(KERBEROS_USER_SERVICE.getDisplayName()).valid(false).explanation("kerberos Principal/Password와 Kerberos User Service는 동시에 설정할 수 없습니다.").build());
        }

        if (kerberosUserService != null && kerberosCredentialsService != null) {
            results.add(new ValidationResult.Builder().subject(KERBEROS_USER_SERVICE.getDisplayName()).valid(false).explanation("kerberos User Service와 Kerberos Credential Service는 동시에 설정할 수 없습니다.").build());
        }

        return results;
    }

    @OnStopped
    public void shutdown() throws Exception {
        try {
            if (this.kuduClient != null) {
                getLogger().debug("Kudu 클라이언트를 종료하는 중입니다.");
                this.kuduClient.close();
                this.kuduClient = null;
            }
        } finally {
            if (kerberosUser != null) {
                kerberosUser.logout();
                kerberosUser = null;
            }
        }
    }

    protected void buildPartialRow(Schema schema,
                                   PartialRow row,
                                   Record record,
                                   List<String> fieldNames,
                                   boolean ignoreNull,
                                   boolean lowercaseFields,
                                   int addHour,
                                   TimestampFormatHolder holder,
                                   String defaultTimestampPatterns) {

        for (String recordFieldName : fieldNames) {
            String colName = recordFieldName;
            if (lowercaseFields) {
                colName = colName.toLowerCase();
            }

            if (!schema.hasColumn(colName)) {
                continue;
            }

            final int columnIndex = schema.getColumnIndex(colName);
            final ColumnSchema colSchema = schema.getColumnByIndex(columnIndex);
            final Type colType = colSchema.getType();

            if (record.getValue(recordFieldName) == null) {
                if (schema.getColumnByIndex(columnIndex).isKey()) {
                    throw new IllegalArgumentException(String.format("Primary Key 컬럼 '%s'은 NULL로 설정할 수 없습니다.", colName));
                } else if (!schema.getColumnByIndex(columnIndex).isNullable()) {
                    throw new IllegalArgumentException(String.format("컬럼 '%s'를 NULL로 설정할 수 없습니다.", colName));
                }

                if (!ignoreNull) {
                    row.setNull(colName);
                }
            } else {
                Object value = record.getValue(recordFieldName);
                final Optional<DataType> fieldDataType = record.getSchema().getDataType(recordFieldName);

                // JSON 형식의 컬럼별 Timestamp Format을 지정하지 않으면 기본 포맷을 사용하거나 Avro의 Timestamp Format을 사용한다.
                String defaultPattern = defaultTimestampPatterns == null ? fieldDataType.map(DataType::getFormat).orElse(null) : defaultTimestampPatterns;
                String finalTimestampPattern = (holder == null ? defaultPattern : holder.getPattern(recordFieldName));
                getLogger().debug("{}", String.format("Record Field Name : {} ==> Type : {}, Date Format : {}", recordFieldName, colType, finalTimestampPattern));

                switch (colType) {
                    case BOOL:
                        row.addBoolean(columnIndex, DataTypeUtils.toBoolean(value, recordFieldName));
                        break;
                    case INT8:
                        row.addByte(columnIndex, DataTypeUtils.toByte(value, recordFieldName));
                        break;
                    case INT16:
                        row.addShort(columnIndex, DataTypeUtils.toShort(value, recordFieldName));
                        break;
                    case INT32:
                        row.addInt(columnIndex, DataTypeUtils.toInteger(value, recordFieldName));
                        break;
                    case INT64:
                        row.addLong(columnIndex, DataTypeUtils.toLong(value, recordFieldName));
                        break;
                    case UNIXTIME_MICROS:
                        Optional<String> optionalPattern = Optional.of(finalTimestampPattern);
/*
                        final Optional<DataType> optionalDataType = record.getSchema().getDataType(recordFieldName);
                        if (holder.getPattern(recordFieldName) == null) {
                            optionalPattern = getTimestampPattern(optionalDataType);
                        } else {
                        optionalPattern = Optional.of(finalTimestampPattern);
                        }
*/
                        getLogger().debug("{}", String.format("[Timestamp] Record Field Name : {} ==> Type : {}, Date Format : {}", recordFieldName, colType, finalTimestampPattern));
                        final Timestamp timestamp = TIMESTAMP_FIELD_CONVERTER.convertField(value, optionalPattern, recordFieldName, addHour, finalTimestampPattern);
                        row.addTimestamp(columnIndex, timestamp);
                        break;
                    case STRING:
                        row.addString(columnIndex, DataTypeUtils.toString(value, finalTimestampPattern));
                        break;
                    case BINARY:
                        row.addBinary(columnIndex, DataTypeUtils.toString(value, finalTimestampPattern).getBytes());
                        break;
                    case FLOAT:
                        row.addFloat(columnIndex, DataTypeUtils.toFloat(value, recordFieldName));
                        break;
                    case DOUBLE:
                        row.addDouble(columnIndex, DataTypeUtils.toDouble(value, recordFieldName));
                        break;
                    case DECIMAL:
                        row.addDecimal(columnIndex, new BigDecimal(DataTypeUtils.toString(value, finalTimestampPattern)));
                        break;
                    case VARCHAR:
                        row.addVarchar(columnIndex, DataTypeUtils.toString(value, finalTimestampPattern));
                        break;
                    case DATE:
                        final String dateFormat = finalTimestampPattern == null ? RecordFieldType.DATE.getDefaultFormat() : finalTimestampPattern;
                        row.addDate(columnIndex, getDate(value, recordFieldName, dateFormat));
                        break;
                    default:
                        throw new IllegalStateException(String.format("'%s'은 알 수 없는 컬럼형", colType));
                }
            }
        }
    }

    /**
     * Get Timestamp Pattern and override Timestamp Record Field pattern with optional microsecond pattern
     *
     * @param optionalDataType Optional Data Type
     * @return Optional Timestamp Pattern
     */
    private Optional<String> getTimestampPattern(final Optional<DataType> optionalDataType) {
        String pattern = null;
        if (optionalDataType.isPresent()) {
            final DataType dataType = optionalDataType.get();
            if (RecordFieldType.TIMESTAMP == dataType.getFieldType()) {
                pattern = MICROSECOND_TIMESTAMP_PATTERN;
            } else {
                pattern = dataType.getFormat();
            }
        }
        return Optional.ofNullable(pattern);
    }

    /**
     * Get java.sql.Date from Record Field Value with optional parsing when input value is a String
     *
     * @param value           Record Field Value
     * @param recordFieldName Record Field Name
     * @param format          Date Format Pattern
     * @return Date object or null when value is null
     */
    private Date getDate(final Object value, final String recordFieldName, final String format) {
        final LocalDate localDate = DataTypeUtils.toLocalDate(value, () -> DataTypeUtils.getDateTimeFormatter(format, ZoneId.systemDefault()), recordFieldName);
        return Date.valueOf(localDate);
    }

    /**
     * NiFi DataType을 Kudu Type으로 변환합니다.
     */
    private Type toKuduType(DataType nifiType) {
        switch (nifiType.getFieldType()) {
            case BOOLEAN:
                return Type.BOOL;
            case BYTE:
                return Type.INT8;
            case SHORT:
                return Type.INT16;
            case INT:
                return Type.INT32;
            case LONG:
                return Type.INT64;
            case FLOAT:
                return Type.FLOAT;
            case DOUBLE:
                return Type.DOUBLE;
            case DECIMAL:
                return Type.DECIMAL;
            case TIMESTAMP:
                return Type.UNIXTIME_MICROS;
            case CHAR:
            case STRING:
                return Type.STRING;
            case DATE:
                return Type.DATE;
            default:
                throw new IllegalArgumentException(String.format("'%s'은 지원하지 않는 자료형", nifiType));
        }
    }

    private ColumnTypeAttributes getKuduTypeAttributes(final DataType nifiType) {
        if (nifiType.getFieldType().equals(RecordFieldType.DECIMAL)) {
            final DecimalDataType decimalDataType = (DecimalDataType) nifiType;
            return new ColumnTypeAttributes.ColumnTypeAttributesBuilder().precision(decimalDataType.getPrecision()).scale(decimalDataType.getScale()).build();
        } else {
            return null;
        }
    }

    /**
     * Based on NiFi field declaration, generates an alter statement to extend table with new column. Note: simply calling
     * {@link AlterTableOptions#addNullableColumn(String, Type)} is not sufficient as it does not cover BigDecimal scale and precision handling.
     *
     * @param columnName 새로운 컬럼명
     * @param nifiType   필드 타입
     * @return 새로운 필드명을 포함하는 Alter Table 구문
     */
    protected AlterTableOptions getAddNullableColumnStatement(final String columnName, final DataType nifiType) {
        final AlterTableOptions alterTable = new AlterTableOptions();
        alterTable.addColumn(new ColumnSchema.ColumnSchemaBuilder(columnName, toKuduType(nifiType)).nullable(true).defaultValue(null).typeAttributes(getKuduTypeAttributes(nifiType)).build());
        return alterTable;
    }

    private static class ClientThreadFactory implements ThreadFactory {
        private final ThreadFactory defaultThreadFactory = Executors.defaultThreadFactory();

        private final AtomicInteger threadCount = new AtomicInteger();

        private final String identifier;

        private ClientThreadFactory(final String identifier) {
            this.identifier = identifier;
        }

        /**
         * 지정한 이름으로 Daemon Thread를 생성합니다.
         *
         * @param runnable Runnable
         * @return Created Thread
         */
        @Override
        public Thread newThread(final Runnable runnable) {
            final Thread thread = defaultThreadFactory.newThread(runnable);
            thread.setDaemon(true);
            thread.setName(getName());
            return thread;
        }

        private String getName() {
            return String.format("PutKudu[%s]-client-%d", identifier, threadCount.getAndIncrement());
        }
    }
}
