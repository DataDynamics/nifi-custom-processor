/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.datadynamics.nifi.kudu;

import com.fasterxml.jackson.databind.ObjectMapper;
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
@Tags({"dd", "custom", "put", "database", "NoSQL", "kudu", "HDFS", "record", "korea"})
@CapabilityDescription("지정한 Record Reader를 사용하여 Incoming FlowFile에서 레코드를 읽고 해당 레코드를 지정된 Kudu의 테이블에 기록합니다. " +
        "Kudu 테이블의 스키마는 Record Reader의 스키마를 활용합니다. " +
        "입력에서 레코드를 읽거나 Kudu에 레코드를 쓰는 동안 오류가 발생하면 FlowFile이 failure로 라우팅됩니다.")
@WritesAttribute(attribute = "record.count", description = "Kudu에 기록한 레코드 수")
public class PutKudu extends AbstractKuduProcessor {

    public static final PropertyDescriptor RECORD_READER = new Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("FlowFile의 레코드를 읽기 위한 Record Reader 서비스를 지정하십시오.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final String RECORD_COUNT_ATTR = "record.count";
    protected static final Validator JsonValidator = new JsonValidator();
    protected static final ObjectMapper mapper = new ObjectMapper();
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
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
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

            final String explanation = valid ? null :
                    "다음의 값중 하나가 되어야 합니다: " + Arrays.stream(OperationType.values()).map(Enum::toString).collect(Collectors.joining(", "));
            return new ValidationResult.Builder().subject(subject).input(value).valid(valid)
                    .explanation(explanation).build();
        }
    };
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
    protected static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFile은 Kudu에 데이터가 성공적으로 저장된 후 이 관계로 라우팅됩니다.")
            .build();
    protected static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFile을 Kudu에 저장할 수 없는 경우 이 관계로 라우팅됩니다.")
            .build();
    static final AllowableValue FAILURE_STRATEGY_ROUTE = new AllowableValue("route-to-failure", "Route to Failure",
            "The FlowFile containing the Records that failed to insert will be routed to the 'failure' relationship");
    static final AllowableValue FAILURE_STRATEGY_ROLLBACK = new AllowableValue("rollback", "Rollback Session",
            "If any Record cannot be inserted, all FlowFiles in the session will be rolled back to their input queue. This means that if data cannot be pushed, " +
                    "it will block any subsequent data from be pushed to Kudu as well until the issue is resolved. However, this may be advantageous if a strict ordering is required.");
    static final PropertyDescriptor FAILURE_STRATEGY = new Builder()
            .name("failure-strategy")
            .displayName("실패 처리 방법")
            .description("배치에서 하나 이상의 레코드를 Kudu로 전송할 수 없는 경우 실패 처리 방법을 지정합니다.")
            .required(true)
            .allowableValues(FAILURE_STRATEGY_ROUTE, FAILURE_STRATEGY_ROLLBACK)
            .defaultValue(FAILURE_STRATEGY_ROUTE.getValue())
            .build();
    static final PropertyDescriptor DATA_RECORD_PATH = new Builder()
            .name("data-recordpath")
            .displayName("Data RecordPath")
            .description("If specified, this property denotes a RecordPath that will be evaluated against each incoming Record and the Record that results from evaluating the RecordPath will be sent to Kudu instead of sending the entire incoming Record. If not specified, the entire incoming Record will be published to Kudu.")
            .required(false)
            .addValidator(new RecordPathValidator())
            .expressionLanguageSupported(NONE)
            .build();
    static final PropertyDescriptor OPERATION_RECORD_PATH = new Builder()
            .name("operation-recordpath")
            .displayName("Operation RecordPath")
            .description("If specified, this property denotes a RecordPath that will be evaluated against each incoming Record in order to determine the Kudu Operation Type. When evaluated, the RecordPath must evaluate to one of hte valid Kudu Operation Types, or the incoming FlowFile will be routed to failure. If this property is specified, the <Kudu Operation Type> property will be ignored.")
            .required(false)
            .addValidator(new RecordPathValidator())
            .expressionLanguageSupported(NONE)
            .build();
    static final PropertyDescriptor CUSTOM_COLUMN_TIMESTAMP_PATTERNS = new Builder()
            .name("kudu-column-timestamp-patterns")
            .displayName("Timestamp 컬럼의 Timestamp Format을 적용합니다. 입력값은 JSON 형식입니다.")
            .description("Timestamp 컬럼에 Timestamp Format을 별도로 지정할 수 있습니다. Timestamp Format은 microseconds까지만 지원합니다.")
            .required(false)
            .addValidator(JsonValidator)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    // Properties set in onScheduled.
    private volatile int batchSize = 100;
    private volatile int ffbatch = 1;

    private volatile int addHour = 0;
    private volatile SessionConfiguration.FlushMode flushMode;
    private volatile Function<Record, OperationType> recordPathOperationType;
    private volatile RecordPath dataRecordPath;
    private volatile String failureStrategy;
    private volatile boolean supportsInsertIgnoreOp;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(KUDU_MASTERS);
        properties.add(TABLE_NAME);
        properties.add(FAILURE_STRATEGY);
        properties.add(LOWERCASE_FIELD_NAMES);
        properties.add(HANDLE_SCHEMA_DRIFT);
        properties.add(RECORD_READER);
        properties.add(DATA_RECORD_PATH);
        properties.add(OPERATION_RECORD_PATH);
        properties.add(INSERT_OPERATION);
        properties.add(FLUSH_MODE);
        properties.add(FLOWFILE_BATCH_SIZE);
        properties.add(BATCH_SIZE);
        properties.add(IGNORE_NULL);
        properties.add(WORKER_COUNT);
        properties.add(ADD_HOUR);
        properties.add(KUDU_OPERATION_TIMEOUT_MS);
        properties.add(KUDU_KEEP_ALIVE_PERIOD_TIMEOUT_MS);
        properties.add(KUDU_SASL_PROTOCOL_NAME);
        properties.add(KERBEROS_USER_SERVICE);
        properties.add(KERBEROS_CREDENTIALS_SERVICE);
        properties.add(KERBEROS_PRINCIPAL);
        properties.add(KERBEROS_PASSWORD);
        properties.add(CUSTOM_COLUMN_TIMESTAMP_PATTERNS);
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

        final String operationRecordPathValue = context.getProperty(OPERATION_RECORD_PATH).getValue();
        if (operationRecordPathValue == null) {
            recordPathOperationType = null;
        } else {
            final RecordPath recordPath = RecordPath.compile(operationRecordPathValue);
            recordPathOperationType = new RecordPathOperationType(recordPath);
        }

        final String dataRecordPathValue = context.getProperty(DATA_RECORD_PATH).getValue();
        dataRecordPath = dataRecordPathValue == null ? null : RecordPath.compile(dataRecordPathValue);

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

        String customTimestampPatterns = context.getProperty(CUSTOM_COLUMN_TIMESTAMP_PATTERNS).evaluateAttributeExpressions().getValue();
        getLogger().info("Custom Timestamp Format : \n{}", customTimestampPatterns);

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
            executeOnKuduClient(kuduClient -> processFlowFiles(context, session, flowFiles, kuduClient, finalHolder));
            return;
        }

        final PrivilegedExceptionAction<Void> privilegedAction = () -> {
            executeOnKuduClient(kuduClient -> processFlowFiles(context, session, flowFiles, kuduClient, finalHolder));
            return null;
        };

        final KerberosAction<Void> action = new KerberosAction<>(user, privilegedAction, getLogger());
        action.execute();
    }

    private void processFlowFiles(final ProcessContext context, final ProcessSession session, final List<FlowFile> flowFiles, final KuduClient kuduClient, TimestampFormatHolder holder) {
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
                    holder);
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
                                final TimestampFormatHolder holder) {

        final RecordReaderFactory recordReaderFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);

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
                if (recordPathOperationType == null) {
                    final OperationType staticOperationType = OperationType.valueOf(getEvaluatedProperty(INSERT_OPERATION, context, flowFile).toUpperCase());
                    operationTypeFunction = record -> staticOperationType;
                } else {
                    operationTypeFunction = recordPathOperationType;
                }

                final RecordSet recordSet = recordReader.createRecordSet();
                KuduTable kuduTable = kuduClient.openTable(tableName);

                // Get the first record so that we can evaluate the Kudu table for Schema drift.
                Record record = recordSet.next();

                // If handleSchemaDrift is true, check for any missing columns and alter the Kudu table to add them.
                if (handleSchemaDrift) {
                    final boolean driftDetected = handleSchemaDrift(kuduTable, kuduClient, flowFile, record, lowercaseFields);

                    if (driftDetected) {
                        // Re-open the table to get the new schema.
                        kuduTable = kuduClient.openTable(tableName);
                    }
                }

                recordReaderLoop:
                while (record != null) {
                    final OperationType operationType = operationTypeFunction.apply(record);

                    final List<Record> dataRecords;
                    if (dataRecordPath == null) {
                        dataRecords = Collections.singletonList(record);
                    } else {
                        final RecordPathResult result = dataRecordPath.evaluate(record);
                        final List<FieldValue> fieldValues = result.getSelectedFields().collect(Collectors.toList());
                        if (fieldValues.isEmpty()) {
                            throw new ProcessException("RecordPath " + dataRecordPath.getPath() + " evaluated against Record yielded no results.");
                        }

                        for (final FieldValue fieldValue : fieldValues) {
                            final RecordFieldType fieldType = fieldValue.getField().getDataType().getFieldType();
                            if (fieldType != RecordFieldType.RECORD) {
                                throw new ProcessException("RecordPath " + dataRecordPath.getPath() + " evaluated against Record expected to return one or more Records but encountered field of type" +
                                        " " + fieldType);
                            }
                        }

                        dataRecords = new ArrayList<>(fieldValues.size());
                        for (final FieldValue fieldValue : fieldValues) {
                            dataRecords.add((Record) fieldValue.getValue());
                        }
                    }

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
                        Operation operation = createKuduOperation(operationType, dataRecord, fieldNames, ignoreNull, lowercaseFields, kuduTable, holder);
                        // We keep track of mappings between Operations and their origins,
                        // so that we know which FlowFiles should be marked failure after buffered flush.
                        operationFlowFileMap.put(operation, flowFile);

                        // Flush mutation buffer of KuduSession to avoid "MANUAL_FLUSH is enabled
                        // but the buffer is too big" error. This can happen when flush mode is
                        // MANUAL_FLUSH and a FlowFile has more than one records.
                        if (bufferedRecords == batchSize && flushMode == SessionConfiguration.FlushMode.MANUAL_FLUSH) {
                            bufferedRecords = 0;
                            flushKuduSession(kuduSession, false, pendingRowErrors);
                        }

                        // OperationResponse is returned only when flush mode is set to AUTO_FLUSH_SYNC
                        OperationResponse response = kuduSession.apply(operation);
                        if (response != null && response.hasRowError()) {
                            // Stop processing the records on the first error.
                            // Note that Kudu does not support rolling back of previous operations.
                            flowFileFailures.put(flowFile, response.getRowError());
                            break recordReaderLoop;
                        }

                        bufferedRecords++;
                        processedRecords.merge(flowFile, 1, Integer::sum);
                    }

                    record = recordSet.next();
                }
            } catch (Exception ex) {
                getLogger().error("Failed to push {} to Kudu", new Object[]{flowFile}, ex);
                flowFileFailures.put(flowFile, ex);
            }
        }
    }

    private boolean handleSchemaDrift(final KuduTable kuduTable, final KuduClient kuduClient, final FlowFile flowFile, final Record record, final boolean lowercaseFields) {
        if (record == null) {
            getLogger().debug("No Record to evaluate schema drift against for {}", flowFile);
            return false;
        }

        final String tableName = kuduTable.getName();
        final Schema schema = kuduTable.getSchema();

        final List<RecordField> recordFields;
        if (dataRecordPath == null) {
            recordFields = record.getSchema().getFields();
        } else {
            final RecordPathResult recordPathResult = dataRecordPath.evaluate(record);
            final List<FieldValue> fieldValues = recordPathResult.getSelectedFields().collect(Collectors.toList());

            recordFields = new ArrayList<>();
            for (final FieldValue fieldValue : fieldValues) {
                final RecordField recordField = fieldValue.getField();
                if (recordField.getDataType().getFieldType() == RecordFieldType.RECORD) {
                    final Object value = fieldValue.getValue();
                    if (value instanceof Record) {
                        recordFields.addAll(((Record) value).getSchema().getFields());
                    }
                } else {
                    recordFields.add(recordField);
                }
            }
        }

        final List<RecordField> missing = recordFields.stream()
                .filter(field -> !schema.hasColumn(lowercaseFields ? field.getFieldName().toLowerCase() : field.getFieldName()))
                .collect(Collectors.toList());

        if (missing.isEmpty()) {
            getLogger().debug("No schema drift detected for {}", flowFile);
            return false;
        }

        getLogger().info("Adding {} columns to table '{}' to handle schema drift", missing.size(), tableName);

        // Add each column one at a time to avoid failing if some of the missing columns
        // we created by a concurrent thread or application attempting to handle schema drift.
        for (final RecordField field : missing) {
            try {
                final String columnName = lowercaseFields ? field.getFieldName().toLowerCase() : field.getFieldName();
                kuduClient.alterTable(tableName, getAddNullableColumnStatement(columnName, field.getDataType()));
            } catch (final KuduException e) {
                // Ignore the exception if the column already exists due to concurrent
                // threads or applications attempting to handle schema drift.
                if (e.getStatus().isAlreadyPresent()) {
                    getLogger().info("Column already exists in table '{}' while handling schema drift", tableName);
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
        // Find RowErrors for each FlowFile
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
                rowErrors.forEach(rowError -> getLogger().error("Failed to write due to {}", rowError.toString()));
                flowFile = session.putAttribute(flowFile, RECORD_COUNT_ATTR, Integer.toString(count - rowErrors.size()));
                totalCount -= rowErrors.size(); // Don't include error rows in the the counter.
                session.transfer(flowFile, REL_FAILURE);
            } else {
                flowFile = session.putAttribute(flowFile, RECORD_COUNT_ATTR, String.valueOf(count));

                if (flowFileFailures.containsKey(flowFile)) {
                    getLogger().error("Failed to write due to {}", flowFileFailures.get(flowFile));
                    session.transfer(flowFile, REL_FAILURE);
                } else {
                    session.transfer(flowFile, REL_SUCCESS);
                    session.getProvenanceReporter().send(flowFile, "Successfully added FlowFile to Kudu");
                }
            }
        }

        session.adjustCounter("Records Inserted", totalCount, false);
    }

    private void logFailures(final List<RowError> pendingRowErrors, final Map<Operation, FlowFile> operationFlowFileMap) {
        final Map<FlowFile, List<RowError>> flowFileRowErrors = pendingRowErrors.stream().collect(
                Collectors.groupingBy(e -> operationFlowFileMap.get(e.getOperation())));

        for (final Map.Entry<FlowFile, List<RowError>> entry : flowFileRowErrors.entrySet()) {
            final FlowFile flowFile = entry.getKey();
            final List<RowError> errors = entry.getValue();

            getLogger().error("Could not write {} to Kudu due to: {}", flowFile, errors);
        }
    }

    private String getEvaluatedProperty(PropertyDescriptor property, ProcessContext context, FlowFile flowFile) {
        PropertyValue evaluatedProperty = context.getProperty(property).evaluateAttributeExpressions(flowFile);
        if (property.isRequired() && evaluatedProperty == null) {
            throw new ProcessException(String.format("Property `%s` is required but evaluated to null", property.getDisplayName()));
        }
        return evaluatedProperty.getValue();
    }

    protected KuduSession createKuduSession(final KuduClient client) {
        final KuduSession kuduSession = client.newSession();
        kuduSession.setMutationBufferSpace(batchSize);
        kuduSession.setFlushMode(flushMode);
        return kuduSession;
    }

    protected Operation createKuduOperation(OperationType operationType, Record record,
                                            List<String> fieldNames, boolean ignoreNull,
                                            boolean lowercaseFields, KuduTable kuduTable, TimestampFormatHolder holder) {
        Operation operation;
        switch (operationType) {
            case INSERT:
                operation = kuduTable.newInsert();
                break;
            case INSERT_IGNORE:
                // If the target Kudu cluster does not support ignore operations use an insert.
                // The legacy session based insert ignore will be used instead.
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
                throw new IllegalArgumentException(String.format("OperationType: %s not supported by Kudu", operationType));
        }
        buildPartialRow(kuduTable.getSchema(), operation.getRow(), record, fieldNames, ignoreNull, lowercaseFields, addHour, holder);
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
