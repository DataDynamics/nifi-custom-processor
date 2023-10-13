package io.datadynamics.nifi.kudu.validator;

import org.apache.nifi.components.ValidationResult;
import org.junit.Assert;
import org.junit.Test;

public class JsonValidatorTest {

    @Test
    public void validate_valid() {
        JsonValidator validator = new JsonValidator();
        String json = "{\n" +
                "  \"formats\": [\n" +
                "    {\n" +
                "      \"column-name\": \"COL_TIMESTAMP\",\n" +
                "      \"timestamp-pattern\": \"yyyy-MM-dd HH:mm:ss\",\n" +
                "      \"type\": \"TIMESTAMP_MILLIS\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";
        ValidationResult result = validator.validate("helloworld", json, null);
        Assert.assertTrue(result.isValid());
    }

    @Test
    public void validate_invalid() {
        JsonValidator validator = new JsonValidator();
        String json = "{\n" +
                "  \"formats\": [\n" +
                "    {\n" +
                "      \"column-name\": \"COL_TIMESTAMP\",\n" +
                "      \"timestamp-pattern\": \"yyyy-MM-dd HH:mm:ss\",\n" +
                "      \"type: \"TIMESTAMP_MILLIS\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";
        ValidationResult result = validator.validate("helloworld", json, null);
        Assert.assertFalse(result.isValid());
    }

}