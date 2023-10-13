package io.datadynamics.nifi.kudu.json;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

public class TimestampFormat implements Serializable {

    @JsonProperty("column-name")
    String columnName;

    @JsonProperty("timestamp-pattern")
    String timestampFormat;

    @JsonProperty("type")
    TimestampType type;

    public String getTimestampFormat() {
        return timestampFormat;
    }

    public void setTimestampFormat(String timestampFormat) {
        this.timestampFormat = timestampFormat;
    }

    public TimestampType getType() {
        return type;
    }

    public void setType(TimestampType type) {
        this.type = type;
    }

}
