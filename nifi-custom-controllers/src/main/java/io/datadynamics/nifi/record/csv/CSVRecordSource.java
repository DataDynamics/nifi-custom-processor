package io.datadynamics.nifi.record.csv;

import shaded.org.apache.commons.csv.CSVFormat;
import shaded.org.apache.commons.csv.CSVParser;
import shaded.org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.schema.inference.RecordSource;

import java.io.*;
import java.util.*;

public class CSVRecordSource implements RecordSource<CSVRecordAndFieldNames> {
    private final Iterator<CSVRecord> csvRecordIterator;
    private final List<String> fieldNames;

    public CSVRecordSource(final InputStream in, final PropertyContext context, final Map<String, String> variables) throws IOException {
        final String charset = context.getProperty(CSVUtils.CHARSET).getValue();

        final Reader reader;
        try {
            reader = new InputStreamReader(new BOMInputStream(in), charset);
        } catch (UnsupportedEncodingException e) {
            throw new ProcessException(e);
        }

        final CSVFormat csvFormat = CSVUtils.createCSVFormat(context, variables).withFirstRecordAsHeader().withTrim();
        final CSVParser csvParser = new CSVParser(reader, csvFormat);
        fieldNames = Collections.unmodifiableList(new ArrayList<>(csvParser.getHeaderMap().keySet()));

        csvRecordIterator = csvParser.iterator();
    }

    @Override
    public CSVRecordAndFieldNames next() {
        if (csvRecordIterator.hasNext()) {
            final CSVRecord record = csvRecordIterator.next();
            return new CSVRecordAndFieldNames(record, fieldNames);
        }

        return null;
    }

    public List<String> getFieldNames() {
        return fieldNames;
    }
}
