package io.datadynamics.nifi.kudu.validator;

import org.apache.nifi.components.ValidationResult;
import org.junit.Assert;
import org.junit.Test;

public class TimestampValidatorTest {

    @Test
    public void validate_invalid() {
        TimestampValidator validator = new TimestampValidator();
        ValidationResult result = validator.validate("helloworld", "yasdfyyyMMddHHmmss", null);
        Assert.assertFalse(result.isValid());
    }

    @Test
    public void validate_valid() {
        TimestampValidator validator = new TimestampValidator();
        ValidationResult result = validator.validate("helloworld", "yyyyMMddHHmmss", null);
        Assert.assertTrue(result.isValid());
    }

}