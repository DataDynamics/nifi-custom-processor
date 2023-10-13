package io.datadynamics.nifi.kudu.converter;

import org.apache.commons.lang3.time.DateUtils;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Optional;

public class ObjectTimestampFieldConverterTest {

    @Test
    public void convertField_long() throws ParseException {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String dateString = "2023-01-01 11:11:11.111";
        Date date = formatter.parse(dateString);
        long current = date.getTime();

        ObjectTimestampFieldConverter converter = new ObjectTimestampFieldConverter();
        Timestamp output = converter.convertField(current, Optional.of("yyyy-MM-dd"), "helloworld", 0, "yyyy-MM-dd HH:mm:ss.SSS");
        Assert.assertEquals(current, output.getTime());
    }

    @Test
    public void convertField_string() throws ParseException {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String dateString = "2023-01-01 11:11:11.111";
        Date date = formatter.parse(dateString);
        long current = date.getTime();

        ObjectTimestampFieldConverter converter = new ObjectTimestampFieldConverter();
        Timestamp output = converter.convertField(dateString, Optional.of("yyyy-MM-dd"), "helloworld", 0, "yyyy-MM-dd HH:mm:ss.SSS");
        Assert.assertEquals(current, output.getTime());
    }

    @Test
    public void convertField_string_addhour() throws ParseException {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String dateString = "2023-01-01 11:11:11.111";
        Date date = formatter.parse(dateString);
        long current = date.getTime();

        ObjectTimestampFieldConverter converter = new ObjectTimestampFieldConverter();
        Timestamp output = converter.convertField(dateString, Optional.of("yyyy-MM-dd"), "helloworld", 9, "yyyy-MM-dd HH:mm:ss.SSS");
        Assert.assertEquals(formatter.format(output), formatter.format(DateUtils.addHours(date, +9)));
    }

}