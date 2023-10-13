package io.datadynamics.nifi.kudu.validator;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;

import java.time.format.DateTimeFormatter;

public class TimestampValidator implements Validator {

    @Override
    public ValidationResult validate(String subject, String value, ValidationContext context) {
        try {
            DateTimeFormatter.ofPattern(value);
            return (new ValidationResult.Builder()).valid(true).input(value).subject(subject).build();
        } catch (Exception e) {
            return (new ValidationResult.Builder()).valid(false).input(value).explanation("DateTimeFormatter 또는 SimpleDateFormat으로 변환이 가능한 패턴이어야 합니다. (예; yyyy-MM-dd HH:mm:ss.SSS)").build();
        }
    }

}