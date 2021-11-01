package io.datadynamics.nifi.processor.processors.custom;

import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.*;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.record.path.util.RecordPathCache;
import org.apache.nifi.record.path.validation.RecordPathPropertyNameValidator;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.*;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"update", "record", "generic", "schema", "custom", "timestamp"})
@CapabilityDescription("")
@WritesAttributes({
        @WritesAttribute(attribute = "record.error.message", description = "이 속성은 Reader 또는 Writer에서 실패시 에러 메시지를 제공합니다")
})
public class UpdateTimestampColumn extends AbstractRecordProcessor {

    private static final String FIELD_NAME = "field.name";
    private static final String FIELD_VALUE = "field.value";
    private static final String FIELD_TYPE = "field.type";

    private volatile RecordPathCache recordPathCache;

    /**
     * 문자열 기반의 Record Path 목록
     */
    private volatile List<String> recordPaths;

    static final AllowableValue RECORD_PATH_VALUES = new AllowableValue("record-path-value", "Record Path Value",
            "The value entered for a Property (after Expression Language has been evaluated) is not the literal value to use but rather is a Record Path "
                    + "that should be evaluated against the Record, and the result of the RecordPath will be used to update the Record. Note that if this option is selected, "
                    + "and the Record Path results in multiple values for a given Record, the input FlowFile will be routed to the 'failure' Relationship.");

    static final PropertyDescriptor COLUMN_NAMES = new PropertyDescriptor.Builder()
            .name("timestamp-column-names")
            .displayName("타임스탬프 컬럼명 (comma separator)")
            .description("타임스탬프 형식을 가진 컬럼명 목록을 콤마 구분자를 갖도록 지정하십시오 (예; create_time,update_time)")
            .allowableValues(RECORD_PATH_VALUES)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    /**
     * 정적으로 사용자가 입력할 수 있는 속성에 대한 Property Descriptor를 정의합니다.
     */
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(COLUMN_NAMES);
        return properties;
    }

    /**
     * 동적으로 사용자가 추가할 수 있는 속성에 대한 Property Descriptor를 정의합니다.
     */
    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .description("RecordPath에 일치하는 Record의 타임스탬프 필드의 값을 지정하십시오 : " + propertyDescriptorName)
                .required(false)
                .dynamic(true)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .addValidator(new RecordPathPropertyNameValidator())
                .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final boolean containsDynamic = validationContext.getProperties().keySet().stream().anyMatch(PropertyDescriptor::isDynamic);

        if (containsDynamic) {
            return Collections.emptyList();
        }

        return Collections.singleton(new ValidationResult.Builder()
                .subject("사용자 정의 속성")
                .valid(false)
                .explanation("최소한 1개 이상의 Timestamp 형식의 컬럼에 대한 RecordPath를 지정해야 합니다")
                .build());
    }

    @OnScheduled
    public void createRecordPaths(final ProcessContext context) {
        recordPathCache = new RecordPathCache(context.getProperties().size() * 2);

        final List<String> recordPaths = new ArrayList<>(context.getProperties().size() - 2);
        for (final PropertyDescriptor property : context.getProperties().keySet()) {
            if (property.isDynamic()) {
                recordPaths.add(property.getName());
            }
        }

        this.recordPaths = recordPaths;
    }

    @Override
    protected Record process(Record record, final FlowFile flowFile, final ProcessContext context, final long count) {
        final boolean evaluateValueAsRecordPath = context.getProperty(COLUMN_NAMES).getValue().equals(RECORD_PATH_VALUES.getValue());

        // 사용자가 입력한 모든 RecordPath 문자열을 추출
        for (final String recordPathText : recordPaths) {
            // RecordPath를 컴파일하고 그 결과를 반환
            final RecordPath recordPath = recordPathCache.getCompiled(recordPathText);
            final RecordPathResult result = recordPath.evaluate(record);

            if (evaluateValueAsRecordPath) {
                final String replacementValue = context.getProperty(recordPathText).evaluateAttributeExpressions(flowFile).getValue();
                final RecordPath replacementRecordPath = recordPathCache.getCompiled(replacementValue);

                // If we have an Absolute RecordPath, we need to evaluate the RecordPath only once against the Record.
                // If the RecordPath is a Relative Path, then we have to evaluate it against each FieldValue.
                if (replacementRecordPath.isAbsolute()) {
                    record = processAbsolutePath(replacementRecordPath, result.getSelectedFields(), record);
                } else {
                    record = processRelativePath(replacementRecordPath, result.getSelectedFields(), record);
                }
            } else {
                final PropertyValue replacementValue = context.getProperty(recordPathText);

                if (replacementValue.isExpressionLanguagePresent()) {
                    final Map<String, String> fieldVariables = new HashMap<>();

                    result.getSelectedFields().forEach(fieldVal -> {
                        fieldVariables.clear();
                        fieldVariables.put(FIELD_NAME, fieldVal.getField().getFieldName());
                        fieldVariables.put(FIELD_VALUE, DataTypeUtils.toString(fieldVal.getValue(), (String) null));
                        fieldVariables.put(FIELD_TYPE, fieldVal.getField().getDataType().getFieldType().name());

                        final String evaluatedReplacementVal = replacementValue.evaluateAttributeExpressions(flowFile, fieldVariables).getValue();
                        fieldVal.updateValue(evaluatedReplacementVal, RecordFieldType.STRING.getDataType());
                    });
                } else {
                    final String evaluatedReplacementVal = replacementValue.evaluateAttributeExpressions(flowFile).getValue();
                    result.getSelectedFields().forEach(fieldVal -> fieldVal.updateValue(evaluatedReplacementVal, RecordFieldType.STRING.getDataType()));
                }
            }
        }

        record.incorporateInactiveFields();

        return record;
    }

    private Record processAbsolutePath(final RecordPath replacementRecordPath, final Stream<FieldValue> destinationFields, final Record record) {
        final RecordPathResult replacementResult = replacementRecordPath.evaluate(record);
        final List<FieldValue> selectedFields = replacementResult.getSelectedFields().collect(Collectors.toList());
        final List<FieldValue> destinationFieldValues = destinationFields.collect(Collectors.toList());

        return updateRecord(destinationFieldValues, selectedFields, record);
    }

    private Record processRelativePath(final RecordPath replacementRecordPath, final Stream<FieldValue> destinationFields, Record record) {
        final List<FieldValue> destinationFieldValues = destinationFields.collect(Collectors.toList());

        for (final FieldValue fieldVal : destinationFieldValues) {
            final RecordPathResult replacementResult = replacementRecordPath.evaluate(record, fieldVal);
            final List<FieldValue> selectedFields = replacementResult.getSelectedFields().collect(Collectors.toList());
            final Object replacementObject = getReplacementObject(selectedFields);
            updateFieldValue(fieldVal, replacementObject);
        }

        return record;
    }

    private Record updateRecord(final List<FieldValue> destinationFields, final List<FieldValue> selectedFields, final Record record) {
        if (destinationFields.size() == 1 && !destinationFields.get(0).getParentRecord().isPresent()) {
            final Object replacement = getReplacementObject(selectedFields);
            if (replacement == null) {
                return record;
            }
            if (replacement instanceof Record) {
                return (Record) replacement;
            }

            final FieldValue replacementFieldValue = (FieldValue) replacement;
            if (replacementFieldValue.getValue() instanceof Record) {
                return (Record) replacementFieldValue.getValue();
            }

            final List<RecordField> fields = selectedFields.stream().map(FieldValue::getField).collect(Collectors.toList());
            final RecordSchema schema = new SimpleRecordSchema(fields);
            final Record mapRecord = new MapRecord(schema, new HashMap<>());
            for (final FieldValue selectedField : selectedFields) {
                mapRecord.setValue(selectedField.getField(), selectedField.getValue());
            }

            return mapRecord;
        } else {
            for (final FieldValue fieldVal : destinationFields) {
                final Object replacementObject = getReplacementObject(selectedFields);
                updateFieldValue(fieldVal, replacementObject);
            }
            return record;
        }
    }

    private void updateFieldValue(final FieldValue fieldValue, final Object replacement) {
        if (replacement instanceof FieldValue) {
            final FieldValue replacementFieldValue = (FieldValue) replacement;
            fieldValue.updateValue(replacementFieldValue.getValue(), replacementFieldValue.getField().getDataType());
        } else {
            fieldValue.updateValue(replacement);
        }
    }

    private Object getReplacementObject(final List<FieldValue> selectedFields) {
        if (selectedFields.size() > 1) {
            final List<RecordField> fields = selectedFields.stream().map(FieldValue::getField).collect(Collectors.toList());
            final RecordSchema schema = new SimpleRecordSchema(fields);
            final Record record = new MapRecord(schema, new HashMap<>());
            for (final FieldValue fieldVal : selectedFields) {
                record.setValue(fieldVal.getField(), fieldVal.getValue());
            }

            return record;
        }

        if (selectedFields.isEmpty()) {
            return null;
        } else {
            return selectedFields.get(0);
        }
    }
}
