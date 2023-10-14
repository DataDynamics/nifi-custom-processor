package io.datadynamics.nifi.kudu;

import org.apache.commons.lang3.RandomUtils;
import org.apache.kudu.shaded.com.google.common.base.Joiner;

import java.io.*;
import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class TestDataGenerator {
    public static void main(String[] args) throws IOException {
        DateTimeFormatter date = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        DateTimeFormatter timestamp1 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        DateTimeFormatter timestamp2 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        DateTimeFormatter timestamp3 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

        FileOutputStream fos = new FileOutputStream("nifi_sample.csv");
        OutputStreamWriter writer = new OutputStreamWriter(fos, "UTF-8");

        int maxRow = 10;
        int i = 0;
        while (i < maxRow) {
            List<String> row = new ArrayList();
            row.add("" + RandomUtils.nextInt()); // COL_INT
            row.add("" + RandomUtils.nextLong()); // COL_FLOAT
            row.add(toString(date)); // COL_DATE
            row.add(toString(timestamp1)); // COL_TIMESTAMP
            row.add(toString(timestamp2)); // COL_TIMESTAMP_MILLIS
            row.add(toString(timestamp3)); // COL_TIMESTAMP_MICROS

            String csv = Joiner.on(",").join(row) + "\n";
            writer.write(csv);

            i++;
        }

        writer.close();
        fos.close();
    }

    public static String toString(DateTimeFormatter formatter) {
        Timestamp timestamp = randomTimestamp();
        return toString(formatter, timestamp);
    }

    public static  String toString(DateTimeFormatter formatter, Timestamp timestamp) {
        return formatter.format(timestamp.toLocalDateTime());
    }

    public static  Timestamp randomTimestamp() {
        long offset = Timestamp.valueOf("2012-01-01 00:00:00").getTime();
        long end = Timestamp.valueOf("2013-01-01 00:00:00").getTime();
        long diff = end - offset + 1;
        return new Timestamp(offset + (long) (Math.random() * diff));
    }
}
