package io.datadynamics.nifi.kudu;

public enum OperationType {
    INSERT,
    INSERT_IGNORE,
    UPSERT,
    UPDATE,
    DELETE,
    UPDATE_IGNORE,
    DELETE_IGNORE
}