package io.datadynamics.nifi.kudu;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;

public class JsonValidator implements Validator {

    private ObjectMapper mapper = new ObjectMapper();

    @Override
    public ValidationResult validate(String subject, String value, ValidationContext context) {
        try {
            mapper.readTree(value.getBytes());
            return (new ValidationResult.Builder()).valid(true).input(value).subject(subject).build();
        } catch (Exception e) {
            return (new ValidationResult.Builder()).valid(false).input(value).explanation("JSON Format이어야 합니다.").build();
        }
    }

}
