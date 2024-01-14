package io.datadynamics.nifi.parquet;

import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

public class ParquetUtilsTest {

    @Test
    void timestampColumns() {
        String avroSchemaJson = "{\n" +
                "  \"type\" : \"record\",\n" +
                "  \"name\" : \"hive_schema\",\n" +
                "  \"fields\" : [ {\n" +
                "    \"name\" : \"registration_dttm\",\n" +
                "    \"type\" : [ \"null\", {\n" +
                "      \"type\" : \"fixed\",\n" +
                "      \"name\" : \"INT96\",\n" +
                "      \"doc\" : \"INT96 represented as byte[12]\",\n" +
                "      \"size\" : 12\n" +
                "    } ],\n" +
                "    \"default\" : null\n" +
                "  }, {\n" +
                "    \"name\" : \"id\",\n" +
                "    \"type\" : [ \"null\", \"int\" ],\n" +
                "    \"default\" : null\n" +
                "  }, {\n" +
                "    \"name\" : \"first_name\",\n" +
                "    \"type\" : [ \"null\", \"string\" ],\n" +
                "    \"default\" : null\n" +
                "  }, {\n" +
                "    \"name\" : \"last_name\",\n" +
                "    \"type\" : [ \"null\", \"string\" ],\n" +
                "    \"default\" : null\n" +
                "  }, {\n" +
                "    \"name\" : \"email\",\n" +
                "    \"type\" : [ \"null\", \"string\" ],\n" +
                "    \"default\" : null\n" +
                "  }, {\n" +
                "    \"name\" : \"gender\",\n" +
                "    \"type\" : [ \"null\", \"string\" ],\n" +
                "    \"default\" : null\n" +
                "  }, {\n" +
                "    \"name\" : \"ip_address\",\n" +
                "    \"type\" : [ \"null\", \"string\" ],\n" +
                "    \"default\" : null\n" +
                "  }, {\n" +
                "    \"name\" : \"cc\",\n" +
                "    \"type\" : [ \"null\", \"string\" ],\n" +
                "    \"default\" : null\n" +
                "  }, {\n" +
                "    \"name\" : \"country\",\n" +
                "    \"type\" : [ \"null\", \"string\" ],\n" +
                "    \"default\" : null\n" +
                "  }, {\n" +
                "    \"name\" : \"birthdate\",\n" +
                "    \"type\" : [ \"null\", \"string\" ],\n" +
                "    \"default\" : null\n" +
                "  }, {\n" +
                "    \"name\" : \"salary\",\n" +
                "    \"type\" : [ \"null\", \"double\" ],\n" +
                "    \"default\" : null\n" +
                "  }, {\n" +
                "    \"name\" : \"title\",\n" +
                "    \"type\" : [ \"null\", \"string\" ],\n" +
                "    \"default\" : null\n" +
                "  }, {\n" +
                "    \"name\" : \"comments\",\n" +
                "    \"type\" : [ \"null\", \"string\" ],\n" +
                "    \"default\" : null\n" +
                "  } ]\n" +
                "}";

        Schema schema = new Schema.Parser().parse(avroSchemaJson);
        String[] columns = ParquetUtils.getTimestampInt96Columns(schema);
        Assert.assertTrue(columns.length == 1);
        Assert.assertEquals("registration_dttm", columns[0]);
    }
}