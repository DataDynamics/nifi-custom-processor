package io.datadynamics.nifi.kudu.json;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TimestampFormatHolder {

    Map<String, TimestampFormat> columns;

    public TimestampFormatHolder(TimestampFormats formats) {
        this.columns = new HashMap<>();
        List<TimestampFormat> formats1 = formats.formats;
        for (TimestampFormat timestampFormat : formats1) {
            this.columns.put(timestampFormat.columnName, timestampFormat);
        }
    }

    public TimestampType getType(String columnName) {
        if (!columns.containsKey(columnName)) {
            return null;
        }
        return columns.get(columnName).getType();
    }

    public String getPattern(String columnName) {
        if (!columns.containsKey(columnName)) {
            return null;
        }
        return columns.get(columnName).getTimestampFormat();
    }

}
