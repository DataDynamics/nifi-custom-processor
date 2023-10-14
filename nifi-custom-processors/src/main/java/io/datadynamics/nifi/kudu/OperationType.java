package io.datadynamics.nifi.kudu;

public enum OperationType {
    INSERT,
    INSERT_IGNORE, // 구버전의 Kudu는 Insert Ignore를 지원하지 않습니다.
    UPSERT,
    UPDATE,
    DELETE,
    UPDATE_IGNORE,
    DELETE_IGNORE
}