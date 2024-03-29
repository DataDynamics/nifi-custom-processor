package io.datadynamics.nifi.kudu.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class TimestampFormatsTest {

    @Test
    public void deserialize() throws JsonProcessingException {
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
        System.out.println(json);
        TimestampFormats timestampFormats = mapper.readValue(json, TimestampFormats.class);

        List<TimestampFormat> formats = timestampFormats.getFormats();
        Assert.assertEquals(1, formats.size());
    }

}
