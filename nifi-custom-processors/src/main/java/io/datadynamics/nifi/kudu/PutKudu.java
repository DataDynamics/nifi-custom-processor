package io.datadynamics.nifi.kudu;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.datadynamics.nifi.kudu.json.TimestampFormatHolder;
import io.datadynamics.nifi.kudu.json.TimestampFormats;
import io.datadynamics.nifi.kudu.validator.JsonValidator;
import io.datadynamics.nifi.kudu.validator.TimestampValidator;
import org.apache.kudu.Schema;
import org.apache.kudu.client.*;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.*;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.record.path.validation.RecordPathValidator;
import org.apache.nifi.security.krb.KerberosAction;
import org.apache.nifi.security.krb.KerberosUser;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.util.StringUtils;

import javax.security.auth.login.LoginException;
import java.io.InputStream;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.nifi.expression.ExpressionLanguageScope.*;

@SystemResourceConsideration(resource = SystemResource.MEMORY)
@EventDriven
@SupportsBatching
@RequiresInstanceClassLoading // Because of calls to UserGroupInformation.setConfiguration
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"timestamp", "custom", "put", "database", "kudu", "record", "gmt"})
@CapabilityDescription("지정한 Record Reader를 사용하여 Incoming FlowFile에서 레코드를 읽고 해당 레코드를 지정된 Kudu의 테이블에 기록합니다. " +
        "Kudu 테이블의 스키마는 Record Reader의 스키마를 활용합니다. " +
        "입력에서 레코드를 읽거나 Kudu에 레코드를 쓰는 동안 오류가 발생하면 FlowFile이 failure로 라우팅됩니다.")
@WritesAttribute(attribute = "record.count", description = "Kudu에 기록한 레코드 수")
public class PutKudu extends AbstractKuduProcessor {

    ///////////////////////////////////////////////
    // Utility
    ///////////////////////////////////////////////

    /**
     * JSON을 파싱하기 위한 Jackson Object Mapper
     */
    protected static final ObjectMapper mapper = new ObjectMapper();

    ///////////////////////////////////////////////
    // Counter
    ///////////////////////////////////////////////

    /**
     * NiFi Web UI Summary에 표시하기 위한 키값
     */
    public static final String RECORD_COUNT_ATTR = "record.count";

    ///////////////////////////////////////////////
    // Relationship
    ///////////////////////////////////////////////

    protected static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFile은 Kudu에 데이터가 성공적으로 저장된 후 이 관계로 라우팅됩니다.")
            .build();
    protected static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFile을 Kudu에 저장할 수 없는 경우 이 관계로 라우팅됩니다.")
            .build();

    ///////////////////////////////////////////////
    // Validator
    ///////////////////////////////////////////////

    /**
     * JSON 형식이 유효한지 검사하는 Validator
     */
    protected static final Validator JsonValidator = new JsonValidator();

    /**
     * Timestamp Pattern이 유효한지 검사하는 Validator
     */
    protected static final Validator timestampValidator = new TimestampValidator();

    /**
     * Kudu Operation Type이 유효한지 확인하는 Validator
     */
    protected static final Validator OperationTypeValidator = new Validator() {
        @Override
        public ValidationResult validate(String subject, String value, ValidationContext context) {
            if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(value)) {
                return new ValidationResult.Builder().subject(subject).input(value).explanation("Expression Language 필요").valid(true).build();
            }

            boolean valid;
            try {
                OperationType.valueOf(value.toUpperCase());
                valid = true;
            } catch (IllegalArgumentException ex) {
                valid = false;
            }

            final String explanation = valid ? null : "다음의 값중 하나가 되어야 합니다: " + Arrays.stream(OperationType.values()).map(Enum::toString).collect(Collectors.joining(", "));
            return new ValidationResult.Builder().subject(subject).input(value).valid(valid).explanation(explanation).build();
        }
    };

    ///////////////////////////////////////////////
    // Allowable Value
    ///////////////////////////////////////////////

    static final AllowableValue FAILURE_STRATEGY_ROUTE = new AllowableValue("route-to-failure",
            "실패로 처리",
            "The FlowFile containing the Records that failed to insert will be routed to the " +
                    "'failure' relationship");
    static final AllowableValue FAILURE_STRATEGY_ROLLBACK = new AllowableValue("rollback",
            "세션 롤백",
            "If any Record cannot be inserted, all FlowFiles in the session will be rolled back " +
                    "to their input queue. This means that if data cannot be pushed, " +
                    "it will block any subsequent data from be pushed to Kudu as well " +
                    "until the issue is resolved. However, this may be advantageous " +
                    "if a strict ordering is required.");

    ///////////////////////////////////////////////
    // Property
    ///////////////////////////////////////////////

    public static final PropertyDescriptor RECORD_READER = new Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("FlowFile의 레코드를 읽기 위한 Record Reader 서비스를 지정하십시오.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    protected static final PropertyDescriptor TABLE_NAME = new Builder()
            .name("kudu-table-name")
            .displayName("데이터를 저장할 Kudu 테이블명")
            .description("Kudu 테이블에 데이터를 저장하기 위해서 테이블명을 지정하십시오.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .build();
    protected static final PropertyDescriptor ADD_HOUR = new Builder()
            .name("add-hour-to-timestamp-column")
            .displayName("Timestamp 컬럼에 시간 추가")
            .description("Timestamp 컬럼에서 GMT, KST 등의 시간 조정을 위해서 추가할 시간을 입력합니다. Kudu는 GMT를 기본 시간으로 사용하므로 GMT로 데이터를 사용할 수 없을때 추가할 시간을 입력합니다. 그러면 Timestamp 컬럼에 대해서 지정한 시간만큼 +하여 저장합니다.")
            .required(false)
            .defaultValue("0")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .expressionLanguageSupported(VARIABLE_REGISTRY)
            .build();
    protected static final PropertyDescriptor LOWERCASE_FIELD_NAMES = new Builder()
            .name("use-lower-case")
            .displayName("소문자 필드명 사용")
            .description("Kudu 테이블 컬럼의 인덱스를 찾을 때 컬럼명을 소문자로 변환하여 처리합니다.")
            .defaultValue("false")
            .required(true)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    protected static final PropertyDescriptor HANDLE_SCHEMA_DRIFT = new Builder()
            .name("handle-schema-drift")
            .displayName("컬럼 누락시 컬럼 추가 처리")
            .description("true로 설정하면 대상 Kudu 테이블에 없는 이름을 가진 필드가 발견되면 Kudu 테이블에 새로운 컬럼을 추가하도록 변경합니다.")
            .defaultValue("false")
            .required(true)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    protected static final PropertyDescriptor INSERT_OPERATION = new Builder()
            .name("Insert Operation")
            .displayName("Kudu Operation 유형")
            .description("Kudu Operation의 유형을 지정하십시오.\n" +
                    "사용 가능한 값: " +
                    Arrays.stream(OperationType.values()).map(Enum::toString).collect(Collectors.joining(", ")) +
                    ". 만약에 <Operation RecordPath>을 설정하는 경우 이 값은 무시합니다.")
            .defaultValue(OperationType.INSERT.toString())
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .addValidator(OperationTypeValidator)
            .build();
    protected static final PropertyDescriptor FLUSH_MODE = new Builder()
            .name("Flush 모드")
            .description("Kudu 세션에 Flush 모드를 설정합니다.\n" +
                    "AUTO_FLUSH_SYNC: the call returns when the operation is persisted, else it throws an exception.\n" +
                    "AUTO_FLUSH_BACKGROUND: the call returns when the operation has been added to the buffer. This call should normally perform only fast in-memory operations but it may have to wait when the buffer is full and there's another buffer being flushed.\n" +
                    "MANUAL_FLUSH: the call returns when the operation has been added to the buffer, else it throws a KuduException if the buffer is full.")
            .allowableValues(SessionConfiguration.FlushMode.values())
            .defaultValue(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND.toString())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();
    protected static final PropertyDescriptor FLOWFILE_BATCH_SIZE = new Builder()
            .name("배치당 FlowFile의 개수")
            .description("단일 실행에서 처리할 최대 FlowFile 수는 1~100000입니다. 메모리 크기 및 행당 데이터 크기에 따라 클라이언트 연결 설정당 처리할 FlowFile 수의 적절한 배치 크기를 설정하십시오. " +
                    "시스템이 수용할 수 있는 수준을 확인하면서 이 값을 점차적으로 늘리십시오. 대부분의 경우 이 값을 1로 설정하도록 하고 Processor의 Concurrent Task를 늘리는 전략이 더 유용합니다.")
            .defaultValue("1")
            .required(true)
            .addValidator(StandardValidators.createLongValidator(1, 100000, true))
            .expressionLanguageSupported(VARIABLE_REGISTRY)
            .build();
    protected static final PropertyDescriptor BATCH_SIZE = new Builder()
            .name("배치당 처리할 ROW 수")
            .displayName("1개의 배치당 최대 ROW 수")
            .description("단일 Kudu 클라이언트가 1회 배치 처리에서 처리할 최대 레코드 수는 1~100000입니다. 메모리 크기 및 행당 데이터 크기에 따라 적절한 일괄 처리 크기를 설정합니다. " +
                    "최고의 성능을 위한 최상의 숫자를 찾으려면 이 숫자를 점차적으로 늘리면서 테스트하여 결정하도록 합니다.")
            .defaultValue("100")
            .required(true)
            .addValidator(StandardValidators.createLongValidator(1, 100000, true))
            .expressionLanguageSupported(VARIABLE_REGISTRY)
            .build();
    protected static final PropertyDescriptor IGNORE_NULL = new Builder()
            .name("NULL 무시")
            .description("Kudu Put 작업시 NULL 무시. true로 설정하면 non-null만 업데이트합니다.")
            .defaultValue("false")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .build();
    static final PropertyDescriptor FAILURE_STRATEGY = new Builder()
            .name("failure-strategy")
            .displayName("실패 처리 방법")
            .description("배치에서 하나 이상의 레코드를 Kudu로 전송할 수 없는 경우 실패 처리 방법을 지정합니다.")
            .required(true)
            .allowableValues(FAILURE_STRATEGY_ROUTE, FAILURE_STRATEGY_ROLLBACK)
            .defaultValue(FAILURE_STRATEGY_ROUTE.getValue())
            .build();
    static final PropertyDescriptor CUSTOM_COLUMN_TIMESTAMP_PATTERNS = new Builder()
            .name("kudu-column-timestamp-patterns")
            .displayName("Timestamp 컬럼의 Timestamp Format(JSON 형식)")
            .description("Timestamp 컬럼에 Timestamp Format을 별도로 지정할 수 있습니다. Timestamp Format은 microseconds까지만 지원합니다.")
            .required(false)
            .addValidator(JsonValidator)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    static final PropertyDescriptor DEFAULT_TIMESTAMP_PATTERN = new Builder()
            .name("default-timestamp-pattern")
            .displayName("기본 날짜 패턴")
            .description("'Timestamp 컬럼의 Timestamp Format(JSON 형식)'으로 컬럼별 파싱 패턴을 지정하지 않으면 Timestamp 컬럼에 이 파상 패턴을 적용합니다.")
            .required(true)
            .defaultValue("yyyy-MM-dd HH:mm:ss.SSS")
            .addValidator(timestampValidator)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .build();
    static final PropertyDescriptor ROW_LOGGING_COUNT = new Builder()
            .name("row-logging-count")
            .displayName("ROW 처리 로깅시 ROW 건수")
            .description("ROW를 처리할때 특정 건수마다 로그를 출력합니다. 0으로 설정하는 경우 출력하지 않습니다.")
            .required(true)
            .defaultValue("1000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .build();

    ///////////////////////////////////////////////
    // onScheduled시 설정하는 값
    ///////////////////////////////////////////////

    private volatile int batchSize = 100;

    private volatile int ffbatch = 1;

    private volatile int addHour = 0;

    private volatile SessionConfiguration.FlushMode flushMode;

    private volatile String failureStrategy;

    private volatile boolean supportsInsertIgnoreOp;

    ///////////////////////////////////////////////
    // Method
    ///////////////////////////////////////////////

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(KUDU_MASTERS);
        properties.add(TABLE_NAME);
        properties.add(FAILURE_STRATEGY);
        properties.add(LOWERCASE_FIELD_NAMES);
        properties.add(HANDLE_SCHEMA_DRIFT);
        properties.add(RECORD_READER);
        properties.add(INSERT_OPERATION);
        properties.add(FLUSH_MODE);
        properties.add(FLOWFILE_BATCH_SIZE);
        properties.add(BATCH_SIZE);
        properties.add(IGNORE_NULL);
        properties.add(WORKER_COUNT);
        properties.add(ADD_HOUR);
        properties.add(KUDU_OPERATION_TIMEOUT_MS);
        properties.add(KUDU_SASL_PROTOCOL_NAME);
        properties.add(KERBEROS_USER_SERVICE);
        properties.add(KERBEROS_CREDENTIALS_SERVICE);
        properties.add(KERBEROS_PRINCIPAL);
        properties.add(KERBEROS_PASSWORD);
        properties.add(CUSTOM_COLUMN_TIMESTAMP_PATTERNS);
        properties.add(DEFAULT_TIMESTAMP_PATTERN);
        properties.add(ROW_LOGGING_COUNT);
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        return rels;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws LoginException {
        batchSize = context.getProperty(BATCH_SIZE).evaluateAttributeExpressions().asInteger();
        ffbatch = context.getProperty(FLOWFILE_BATCH_SIZE).evaluateAttributeExpressions().asInteger();
        addHour = context.getProperty(ADD_HOUR).evaluateAttributeExpressions().asInteger();
        flushMode = SessionConfiguration.FlushMode.valueOf(context.getProperty(FLUSH_MODE).getValue().toUpperCase());
        createKerberosUserAndOrKuduClient(context);
        supportsInsertIgnoreOp = supportsIgnoreOperations();
        failureStrategy = context.getProperty(FAILURE_STRATEGY).getValue();
    }

    private boolean isRollbackOnFailure() {
        return FAILURE_STRATEGY_ROLLBACK.getValue().equalsIgnoreCase(failureStrategy);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final List<FlowFile> flowFiles = session.get(ffbatch);
        if (flowFiles.isEmpty()) {
            return;
        }

        // Timestamp 컬럼을 위해서 Timestamp Format(Pattern)을 지정한 JSON을 로딩한다. 이 파라마터는 FlowFile Attribute를 지원한다.
        String customTimestampPatterns = context.getProperty(CUSTOM_COLUMN_TIMESTAMP_PATTERNS).evaluateAttributeExpressions().getValue();
        String defaultTimestampPatterns = context.getProperty(DEFAULT_TIMESTAMP_PATTERN).evaluateAttributeExpressions().getValue();
        getLogger().info("지정한 Timestamp 패턴(포맷) JSON : \n{}", customTimestampPatterns);
        getLogger().info("기본 Timestamp 패턴(포맷) : {}", defaultTimestampPatterns);

        TimestampFormatHolder holder = null;
        if (!StringUtils.isEmpty(customTimestampPatterns)) {
            try {
                TimestampFormats timestampFormats = mapper.readValue(customTimestampPatterns, TimestampFormats.class);
                holder = new TimestampFormatHolder(timestampFormats);
            } catch (Exception e) {
                getLogger().warn("Timestamp Pattern JSON을 파싱할 수 없습니다. JSON : \n{}", customTimestampPatterns, e);
                session.transfer(flowFiles, REL_FAILURE);
            }
        }

        final TimestampFormatHolder finalHolder = holder;

        final KerberosUser user = getKerberosUser();
        if (user == null) {
            executeOnKuduClient(kuduClient -> processFlowFiles(context, session, flowFiles, kuduClient, finalHolder, defaultTimestampPatterns));
            return;
        }

        final PrivilegedExceptionAction<Void> privilegedAction = () -> {
            executeOnKuduClient(kuduClient -> processFlowFiles(context, session, flowFiles, kuduClient, finalHolder, defaultTimestampPatterns));
            return null;
        };

        final KerberosAction<Void> action = new KerberosAction<>(user, privilegedAction, getLogger());
        action.execute();
    }

    private void processFlowFiles(final ProcessContext context, final ProcessSession session, final List<FlowFile> flowFiles, final KuduClient kuduClient, TimestampFormatHolder holder, String defaultTimestampPatterns) {
        final Map<FlowFile, Integer> processedRecords = new HashMap<>();
        final Map<FlowFile, Object> flowFileFailures = new HashMap<>();
        final Map<Operation, FlowFile> operationFlowFileMap = new HashMap<>();
        final List<RowError> pendingRowErrors = new ArrayList<>();

        final KuduSession kuduSession = createKuduSession(kuduClient);
        try {
            processRecords(flowFiles,
                    processedRecords,
                    flowFileFailures,
                    operationFlowFileMap,
                    pendingRowErrors,
                    session,
                    context,
                    kuduClient,
                    kuduSession,
                    holder,
                    defaultTimestampPatterns);
        } finally {
            try {
                flushKuduSession(kuduSession, true, pendingRowErrors);
            } catch (final KuduException | RuntimeException e) {
                getLogger().error("KuduSession.close() 실패", e);
            }
        }

        if (isRollbackOnFailure() && (!pendingRowErrors.isEmpty() || !flowFileFailures.isEmpty())) {
            logFailures(pendingRowErrors, operationFlowFileMap);
            session.rollback();
            context.yield();
        } else {
            transferFlowFiles(flowFiles, processedRecords, flowFileFailures, operationFlowFileMap, pendingRowErrors, session);
        }
    }

    private void processRecords(final List<FlowFile> flowFiles,
                                final Map<FlowFile, Integer> processedRecords,
                                final Map<FlowFile, Object> flowFileFailures,
                                final Map<Operation, FlowFile> operationFlowFileMap,
                                final List<RowError> pendingRowErrors,
                                final ProcessSession session,
                                final ProcessContext context,
                                final KuduClient kuduClient,
                                final KuduSession kuduSession,
                                final TimestampFormatHolder holder,
                                final String defaultTimestampPatterns) {

        final RecordReaderFactory recordReaderFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        int rowLoggingCount = context.getProperty(ROW_LOGGING_COUNT).evaluateAttributeExpressions().asInteger();

        int bufferedRecords = 0;
        OperationType prevOperationType = OperationType.INSERT;
        for (FlowFile flowFile : flowFiles) {
            try (final InputStream in = session.read(flowFile);
                 final RecordReader recordReader = recordReaderFactory.createRecordReader(flowFile, in, getLogger())) {

                final String tableName = getEvaluatedProperty(TABLE_NAME, context, flowFile);
                final boolean ignoreNull = Boolean.parseBoolean(getEvaluatedProperty(IGNORE_NULL, context, flowFile));
                final boolean lowercaseFields = Boolean.parseBoolean(getEvaluatedProperty(LOWERCASE_FIELD_NAMES, context, flowFile));
                final boolean handleSchemaDrift = Boolean.parseBoolean(getEvaluatedProperty(HANDLE_SCHEMA_DRIFT, context, flowFile));

                final Function<Record, OperationType> operationTypeFunction;
                final OperationType staticOperationType = OperationType.valueOf(getEvaluatedProperty(INSERT_OPERATION, context, flowFile).toUpperCase());
                operationTypeFunction = record -> staticOperationType;

                final RecordSet recordSet = recordReader.createRecordSet();
                KuduTable kuduTable = kuduClient.openTable(tableName);

                // Schema Drift를 처리하기 첫번째 레코드로 Kudu 테이블을 평가한다.
                Record record = recordSet.next();

                // handleSchemaDrift가 true면, 누락된 컬럼을 확인하여 Kudu 테이블에 컬럼을 추가한다.
                if (handleSchemaDrift) {
                    final boolean driftDetected = handleSchemaDrift(kuduTable, kuduClient, flowFile, record, lowercaseFields);

                    if (driftDetected) {
                        // 새로운 Schema를 위해서 테이블을 오픈한다.
                        kuduTable = kuduClient.openTable(tableName);
                    }
                }

                int rowCount = 0;
                recordReaderLoop:
                while (record != null) {
                    final OperationType operationType = operationTypeFunction.apply(record);

                    final List<Record> dataRecords = Collections.singletonList(record); // Data Record Path는 무시함

                    for (final Record dataRecord : dataRecords) {
                        // If supportsIgnoreOps is false, in the case of INSERT_IGNORE the Kudu session
                        // is modified to ignore row errors.
                        // Because the session is shared across flow files, for batching efficiency, we
                        // need to flush when changing to and from INSERT_IGNORE operation types.
                        // This should be removed when the lowest supported version of Kudu supports
                        // ignore operations.
                        if (!supportsInsertIgnoreOp && prevOperationType != operationType
                                && (prevOperationType == OperationType.INSERT_IGNORE || operationType == OperationType.INSERT_IGNORE)) {
                            flushKuduSession(kuduSession, false, pendingRowErrors);
                            kuduSession.setIgnoreAllDuplicateRows(operationType == OperationType.INSERT_IGNORE);
                        }
                        prevOperationType = operationType;

                        final List<String> fieldNames = dataRecord.getSchema().getFieldNames();
                        Operation operation = createKuduOperation(operationType, dataRecord, fieldNames, ignoreNull, lowercaseFields, kuduTable, holder, defaultTimestampPatterns);
                        // We keep track of mappings between Operations and their origins,
                        // so that we know which FlowFiles should be marked failure after buffered flush.
                        operationFlowFileMap.put(operation, flowFile);
                        rowCount++;

                        // Flush mutation buffer of KuduSession to avoid "MANUAL_FLUSH is enabled
                        // but the buffer is too big" error. This can happen when flush mode is
                        // MANUAL_FLUSH and a FlowFile has more than one records.
                        if (bufferedRecords == batchSize && flushMode == SessionConfiguration.FlushMode.MANUAL_FLUSH) {
                            bufferedRecords = 0;
                            flushKuduSession(kuduSession, false, pendingRowErrors);
                        }

                        // Flush 모드가 AUTO_FLUSH_SYNC인 경우에만 OperationResponse을 반환합니다.
                        OperationResponse response = kuduSession.apply(operation);
                        if (response != null && response.hasRowError()) {
                            // 첫 에러 발생시 레코드 처리를 중지합니다.
                            // Kudu는 이전 Operation에 대해서 롤백 기능을 지원하지 않습니다.
                            flowFileFailures.put(flowFile, response.getRowError());
                            break recordReaderLoop;
                        }

                        bufferedRecords++;
                        processedRecords.merge(flowFile, 1, Integer::sum);

                        if (rowLoggingCount > 0 && (rowCount % rowLoggingCount == 0)) {
                            getLogger().info("FlowFile {}은 현재 {}개 ROW를 처리하고 있습니다.", flowFile, rowCount);
                        }
                    }

                    record = recordSet.next();
                }

                if (rowLoggingCount > 0) {
                    getLogger().info("FlowFile {}은 현재 {}개 ROW를 처리하였습니다.", flowFile, rowCount);
                }
            } catch (Exception ex) {
                getLogger().error("FlowFile {}을 Kudu에 저장할 수 없습니다.", new Object[]{flowFile}, ex);
                flowFileFailures.put(flowFile, ex);
            }
        }
    }

    private boolean handleSchemaDrift(final KuduTable kuduTable, final KuduClient kuduClient, final FlowFile flowFile, final Record record, final boolean lowercaseFields) {
        if (record == null) {
            getLogger().debug("FlowFile {}에는 Schema Drift를 처리할 레코드가 없습니다.", flowFile);
            return false;
        }

        final String tableName = kuduTable.getName();
        final Schema schema = kuduTable.getSchema();

        final List<RecordField> recordFields = record.getSchema().getFields(); // Data Record Path는 지원하지 않음

        final List<RecordField> missing = recordFields.stream()
                .filter(field -> !schema.hasColumn(lowercaseFields ? field.getFieldName().toLowerCase() : field.getFieldName()))
                .collect(Collectors.toList());

        if (missing.isEmpty()) {
            getLogger().debug("FlowFile {}에서 탐지한 Schema Drift가 없습니다.", flowFile);
            return false;
        }

        getLogger().info("Schema Drift를 처리하기 위해서 테이블 '{}'에 {}개의 컬럼을 추가합니다.", tableName, missing.size());

        // 존재하지 않는 컬럼이 다수 존재한다면 실패 처리를 회피하기 위해서동시에 각 컬럼을 추가한다.
        // 동시에 처리하는 쓰레드 또는 애플리케이션이 Schema Drift 처리를 시도하는 것을 고려하여 생성함.
        for (final RecordField field : missing) {
            try {
                final String columnName = lowercaseFields ? field.getFieldName().toLowerCase() : field.getFieldName();
                kuduClient.alterTable(tableName, getAddNullableColumnStatement(columnName, field.getDataType()));
            } catch (final KuduException e) {
                // 동시에 쓰레드 또는 애플리케이션에서 Schema Drift를 처리하므로 이미 컬럼이 존재한다면 에러가 발생하고 이 경우 무시한다.
                if (e.getStatus().isAlreadyPresent()) {
                    getLogger().info("Schema Drift를 처리하고 있는 중 테이블 '{}'에 이미 컬럼이 존재하는 상태입니다.", tableName);
                } else {
                    throw new ProcessException(e);
                }
            }
        }

        return true;
    }

    private void transferFlowFiles(final List<FlowFile> flowFiles,
                                   final Map<FlowFile, Integer> processedRecords,
                                   final Map<FlowFile, Object> flowFileFailures,
                                   final Map<Operation, FlowFile> operationFlowFileMap,
                                   final List<RowError> pendingRowErrors,
                                   final ProcessSession session) {
        // 각 FlowFile 마다 RowError를 확인한다.
        // RowError가 많다면 Heap을 많이 소진할 수 있다.
        final Map<FlowFile, List<RowError>> flowFileRowErrors = pendingRowErrors.stream()
                .filter(e -> operationFlowFileMap.get(e.getOperation()) != null)
                .collect(
                        Collectors.groupingBy(e -> operationFlowFileMap.get(e.getOperation()))
                );

        long totalCount = 0L;
        for (FlowFile flowFile : flowFiles) {
            final int count = processedRecords.getOrDefault(flowFile, 0);
            totalCount += count;
            final List<RowError> rowErrors = flowFileRowErrors.get(flowFile);

            if (rowErrors != null) {
                rowErrors.forEach(rowError -> getLogger().error("Kudu에 저장할 수 없습니다. 에러: {}", rowError.toString()));
                flowFile = session.putAttribute(flowFile, RECORD_COUNT_ATTR, Integer.toString(count - rowErrors.size()));
                totalCount -= rowErrors.size(); // 카운터에 에러 ROW를 포함시키지 않는다.
                session.transfer(flowFile, REL_FAILURE);
            } else {
                flowFile = session.putAttribute(flowFile, RECORD_COUNT_ATTR, String.valueOf(count));

                if (flowFileFailures.containsKey(flowFile)) {
                    getLogger().error("FlowFile '{}'을 Kudu에 저장할 수 없습니다.", flowFileFailures.get(flowFile));
                    session.transfer(flowFile, REL_FAILURE);
                } else {
                    session.transfer(flowFile, REL_SUCCESS);
                    session.getProvenanceReporter().send(flowFile, "FlowFile을 Kudu에 성공적으로 저장했습니다.");
                }
            }
        }

        // NiFi UI에 해당 지표로 건수를 표시한다.
        session.adjustCounter("추가한 레코드수", totalCount, false);
    }

    private void logFailures(final List<RowError> pendingRowErrors, final Map<Operation, FlowFile> operationFlowFileMap) {
        final Map<FlowFile, List<RowError>> flowFileRowErrors = pendingRowErrors.stream().collect(
                Collectors.groupingBy(e -> operationFlowFileMap.get(e.getOperation())));

        for (final Map.Entry<FlowFile, List<RowError>> entry : flowFileRowErrors.entrySet()) {
            final FlowFile flowFile = entry.getKey();
            final List<RowError> errors = entry.getValue();

            getLogger().error("Kudu에 FlowFile {}을 기록할 수 없습니다. 에러: {}", flowFile, errors);
        }
    }

    private String getEvaluatedProperty(PropertyDescriptor property, ProcessContext context, FlowFile flowFile) {
        PropertyValue evaluatedProperty = context.getProperty(property).evaluateAttributeExpressions(flowFile);
        if (property.isRequired() && evaluatedProperty == null) {
            throw new ProcessException(String.format("속성 `%s`은 필수로 지정해야 합니다. NULL값은 해석할 수 없습니다.", property.getDisplayName()));
        }
        return evaluatedProperty.getValue();
    }

    /**
     * Kudu 세션을 생성한다.
     */
    protected KuduSession createKuduSession(final KuduClient client) {
        final KuduSession kuduSession = client.newSession();
        kuduSession.setMutationBufferSpace(batchSize);
        kuduSession.setFlushMode(flushMode);
        return kuduSession;
    }

    /**
     * 지정한 Kudu Operation Type에 맞는 Kudu Operation을 생성한다.
     */
    protected Operation createKuduOperation(OperationType operationType,
                                            Record record,
                                            List<String> fieldNames,
                                            boolean ignoreNull,
                                            boolean lowercaseFields,
                                            KuduTable kuduTable,
                                            TimestampFormatHolder holder,
                                            String defaultTimestampPatterns) {
        Operation operation;
        switch (operationType) {
            case INSERT:
                operation = kuduTable.newInsert();
                break;
            case INSERT_IGNORE:
                // Kudu Cluster가 Insert Ignore를 지원하지 않으면 Insert를 사용한다.
                if (!supportsInsertIgnoreOp) {
                    operation = kuduTable.newInsert();
                } else {
                    operation = kuduTable.newInsertIgnore();
                }
                break;
            case UPSERT:
                operation = kuduTable.newUpsert();
                break;
            case UPDATE:
                operation = kuduTable.newUpdate();
                break;
            case UPDATE_IGNORE:
                operation = kuduTable.newUpdateIgnore();
                break;
            case DELETE:
                operation = kuduTable.newDelete();
                break;
            case DELETE_IGNORE:
                operation = kuduTable.newDeleteIgnore();
                break;
            default:
                throw new IllegalArgumentException(String.format("Kudu Operation 유형 '%s'을 Kudu에서 지원하지 않습니다.", operationType));
        }
        buildPartialRow(kuduTable.getSchema(), operation.getRow(), record, fieldNames, ignoreNull, lowercaseFields, addHour, holder, defaultTimestampPatterns);
        return operation;
    }

    private static class RecordPathOperationType implements Function<Record, OperationType> {
        private final RecordPath recordPath;

        public RecordPathOperationType(final RecordPath recordPath) {
            this.recordPath = recordPath;
        }

        @Override
        public OperationType apply(final Record record) {
            final RecordPathResult recordPathResult = recordPath.evaluate(record);
            final List<FieldValue> resultList = recordPathResult.getSelectedFields().distinct().collect(Collectors.toList());
            if (resultList.isEmpty()) {
                throw new ProcessException("Evaluated RecordPath " + recordPath.getPath() + " against Record but got no results");
            }

            if (resultList.size() > 1) {
                throw new ProcessException("Evaluated RecordPath " + recordPath.getPath() + " against Record and received multiple distinct results (" + resultList + ")");
            }

            final String resultValue = String.valueOf(resultList.get(0).getValue());
            try {
                return OperationType.valueOf(resultValue.toUpperCase());
            } catch (final IllegalArgumentException iae) {
                throw new ProcessException("Evaluated RecordPath " + recordPath.getPath() + " against Record to determine Kudu Operation Type but found invalid value: " + resultValue);
            }
        }
    }
}
