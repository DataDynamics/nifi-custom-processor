package io.datadynamics.nifi.db.executesql;

public class SensitiveValueWrapper {

    private final String value;
    private final boolean sensitive;

    public SensitiveValueWrapper(final String value, final boolean sensitive) {
        this.value = value;
        this.sensitive = sensitive;
    }

    public String getValue() {
        return value;
    }

    public boolean isSensitive() {
        return sensitive;
    }
}