package io.datadynamics.nifi.kudu.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class TimestampFormatHolderTest {

    @Test
    public void get() {
        try {
            ObjectMapper mapper = new ObjectMapper();
            String json = "{\n" +
                    "  \"formats\": [\n" +
                    "    {\n" +
                    "      \"column-name\": \"COL_TIMESTAMP\",\n" +
                    "      \"timestamp-pattern\": \"yyyy-MM-dd HH:mm:ss\",\n" +
                    "      \"type\": \"TIMESTAMP_MILLIS\"\n" +
                    "    }\n" +
                    "  ]\n" +
                    "}";
            TimestampFormats timestampFormats = mapper.readValue(json, TimestampFormats.class);
            TimestampFormatHolder holder = new TimestampFormatHolder(timestampFormats);

            Assert.assertEquals("yyyy-MM-dd HH:mm:ss", holder.getPattern("COL_TIMESTAMP"));
        } catch (Exception e) {
            Assert.assertFalse(true);
        }
    }

}