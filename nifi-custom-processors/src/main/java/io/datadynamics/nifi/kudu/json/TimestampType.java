package io.datadynamics.nifi.kudu.json;

public enum TimestampType {

    TIMESTAMP("yyyy-MM-dd HH:mm:ss"), TIMESTAMP_MILLIS("yyyy-MM-dd HH:mm:ss.SSS"), TIMESTAMP_MICROS("yyyy-MM-dd HH:mm:ss.SSSSSS");

    private final String pattern;

    TimestampType(String pattern) {
        this.pattern = pattern;
    }

    public String getPattern() {
        return pattern;
    }

}