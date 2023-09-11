package io.datadynamics.nifi.record.csv;

import org.apache.commons.csv.CSVRecord;

import java.util.List;

public class CSVRecordAndFieldNames {
    private final CSVRecord record;
    private final List<String> fieldNames;

    public CSVRecordAndFieldNames(final CSVRecord record, final List<String> fieldNames) {
        this.record = record;
        this.fieldNames = fieldNames;
    }

    public CSVRecord getRecord() {
        return record;
    }

    public List<String> getFieldNames() {
        return fieldNames;
    }
}
