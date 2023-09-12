/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package shaded.org.apache.commons.csv;

import org.apache.commons.io.input.BOMInputStream;
import org.apache.commons.io.input.BrokenInputStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static shaded.org.apache.commons.csv.Constants.*;

/**
 * CSVParserTest
 * <p>
 * The test are organized in three different sections: The 'setter/getter' section, the lexer section and finally the
 * parser section. In case a test fails, you should follow a top-down approach for fixing a potential bug (its likely
 * that the parser itself fails if the lexer has problems...).
 */
public class CSVParserTest {

    private static final Charset UTF_8 = StandardCharsets.UTF_8;

    private static final String UTF_8_NAME = UTF_8.name();

    private static final String CSV_INPUT = "a,b,c,d\n" + " a , b , 1 2 \n" + "\"foo baar\", b,\n"
            // + " \"foo\n,,\n\"\",,\n\\\"\",d,e\n";
            + "   \"foo\n,,\n\"\",,\n\"\"\",d,e\n"; // changed to use standard CSV escaping

    private static final String CSV_INPUT_1 = "a,b,c,d";

    private static final String CSV_INPUT_2 = "a,b,1 2";

    private static final String[][] RESULT = {{"a", "b", "c", "d"}, {"a", "b", "1 2"}, {"foo baar", "b", ""}, {"foo\n,,\n\",,\n\"", "d", "e"}};

    // CSV with no header comments
    static private final String CSV_INPUT_NO_COMMENT = "A,B" + CRLF + "1,2" + CRLF;

    // CSV with a header comment
    static private final String CSV_INPUT_HEADER_COMMENT = "# header comment" + CRLF + "A,B" + CRLF + "1,2" + CRLF;

    // CSV with a single line header and trailer comment
    static private final String CSV_INPUT_HEADER_TRAILER_COMMENT = "# header comment" + CRLF + "A,B" + CRLF + "1,2" + CRLF + "# comment";

    // CSV with a multi-line header and trailer comment
    static private final String CSV_INPUT_MULTILINE_HEADER_TRAILER_COMMENT = "# multi-line" + CRLF + "# header comment" + CRLF + "A,B" + CRLF + "1,2" + CRLF + "# multi-line" + CRLF + "# comment";

    // Format with auto-detected header
    static private final shaded.org.apache.commons.csv.CSVFormat FORMAT_AUTO_HEADER = shaded.org.apache.commons.csv.CSVFormat.Builder.create(shaded.org.apache.commons.csv.CSVFormat.DEFAULT).setCommentMarker('#').setHeader().build();

    // Format with explicit header
    static private final shaded.org.apache.commons.csv.CSVFormat FORMAT_EXPLICIT_HEADER = shaded.org.apache.commons.csv.CSVFormat.Builder.create(shaded.org.apache.commons.csv.CSVFormat.DEFAULT)
            .setSkipHeaderRecord(true)
            .setCommentMarker('#')
            .setHeader("A", "B")
            .build();

    // Format with explicit header that does not skip the header line
    shaded.org.apache.commons.csv.CSVFormat FORMAT_EXPLICIT_HEADER_NOSKIP = shaded.org.apache.commons.csv.CSVFormat.Builder.create(shaded.org.apache.commons.csv.CSVFormat.DEFAULT)
            .setCommentMarker('#')
            .setHeader("A", "B")
            .build();

    @SuppressWarnings("resource") // caller releases
    private BOMInputStream createBOMInputStream(final String resource) throws IOException {
        return new BOMInputStream(ClassLoader.getSystemClassLoader().getResource(resource).openStream());
    }

    shaded.org.apache.commons.csv.CSVRecord parse(final shaded.org.apache.commons.csv.CSVParser parser, final int failParseRecordNo) throws IOException {
        if (parser.getRecordNumber() + 1 == failParseRecordNo) {
            assertThrows(IOException.class, () -> parser.nextRecord());
            return null;
        }
        return parser.nextRecord();
    }

    private void parseFully(final shaded.org.apache.commons.csv.CSVParser parser) {
        parser.forEach(Assertions::assertNotNull);
    }

    @Test
    public void testBackslashEscaping() throws IOException {

        // To avoid confusion over the need for escaping chars in java code,
        // We will test with a forward slash as the escape char, and a single
        // quote as the encapsulator.

        final String code = "one,two,three\n" // 0
                + "'',''\n" // 1) empty encapsulators
                + "/',/'\n" // 2) single encapsulators
                + "'/'','/''\n" // 3) single encapsulators encapsulated via escape
                + "'''',''''\n" // 4) single encapsulators encapsulated via doubling
                + "/,,/,\n" // 5) separator escaped
                + "//,//\n" // 6) escape escaped
                + "'//','//'\n" // 7) escape escaped in encapsulation
                + "   8   ,   \"quoted \"\" /\" // string\"   \n" // don't eat spaces
                + // escaped newline
                "9,   /\n   \n";
        final String[][] res = {{"one", "two", "three"}, // 0
                {"", ""}, // 1
                {"'", "'"}, // 2
                {"'", "'"}, // 3
                {"'", "'"}, // 4
                {",", ","}, // 5
                {"/", "/"}, // 6
                {"/", "/"}, // 7
                {"   8   ", "   \"quoted \"\" /\" / string\"   "}, {"9", "   \n   "},};

        final shaded.org.apache.commons.csv.CSVFormat format = shaded.org.apache.commons.csv.CSVFormat.newFormat(',').withQuote('\'').withRecordSeparator(CRLF).withEscape('/').withIgnoreEmptyLines();

        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse(code, format)) {
            final List<shaded.org.apache.commons.csv.CSVRecord> records = parser.getRecords();
            assertFalse(records.isEmpty());

            Utils.compare("Records do not match expected result", res, records);
        }
    }

    @Test
    public void testBackslashEscaping2() throws IOException {

        // To avoid confusion over the need for escaping chars in java code,
        // We will test with a forward slash as the escape char, and a single
        // quote as the encapsulator.

        final String code = " , , \n" // 1)
                + " \t ,  , \n" // 2)
                + // 3)
                " // , /, , /,\n";
        final String[][] res = {{" ", " ", " "}, // 1
                {" \t ", "  ", " "}, // 2
                {" / ", " , ", " ,"}, // 3
        };

        final shaded.org.apache.commons.csv.CSVFormat format = shaded.org.apache.commons.csv.CSVFormat.newFormat(',').withRecordSeparator(CRLF).withEscape('/').withIgnoreEmptyLines();

        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse(code, format)) {
            final List<shaded.org.apache.commons.csv.CSVRecord> records = parser.getRecords();
            assertFalse(records.isEmpty());

            Utils.compare("", res, records);
        }
    }

    @Test
    public void testBOMInputStream_ParserWithReader() throws IOException {
        try (final Reader reader = new InputStreamReader(createBOMInputStream("shaded/org/apache/commons/csv/CSVFileParser/bom.csv"), UTF_8_NAME);
             final shaded.org.apache.commons.csv.CSVParser parser = new shaded.org.apache.commons.csv.CSVParser(reader, shaded.org.apache.commons.csv.CSVFormat.EXCEL.withHeader())) {
            parser.forEach(record -> assertNotNull(record.get("Date")));
        }
    }

    @Test
    public void testCarriageReturnEndings() throws IOException {
        final String code = "foo\rbaar,\rhello,world\r,kanu";
        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse(code, shaded.org.apache.commons.csv.CSVFormat.DEFAULT)) {
            final List<shaded.org.apache.commons.csv.CSVRecord> records = parser.getRecords();
            assertEquals(4, records.size());
        }
    }

    @Test
    public void testCarriageReturnLineFeedEndings() throws IOException {
        final String code = "foo\r\nbaar,\r\nhello,world\r\n,kanu";
        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse(code, shaded.org.apache.commons.csv.CSVFormat.DEFAULT)) {
            final List<shaded.org.apache.commons.csv.CSVRecord> records = parser.getRecords();
            assertEquals(4, records.size());
        }
    }

    @Test
    public void testClose() throws Exception {
        final Reader in = new StringReader("# comment\na,b,c\n1,2,3\nx,y,z");
        final Iterator<shaded.org.apache.commons.csv.CSVRecord> records;
        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withCommentMarker('#').withHeader().parse(in)) {
            records = parser.iterator();
            assertTrue(records.hasNext());
        }
        assertFalse(records.hasNext());
        assertThrows(NoSuchElementException.class, records::next);
    }

    private void testCSV141Failure(final shaded.org.apache.commons.csv.CSVFormat format, final int failParseRecordNo) throws IOException {
        final Path path = Paths.get("src/test/resources/shaded/org/apache/commons/csv/CSV-141/csv-141.csv");
        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse(path, StandardCharsets.UTF_8, format)) {
            // row 1
            shaded.org.apache.commons.csv.CSVRecord record = parse(parser, failParseRecordNo);
            if (record == null) {
                return; // expected failure
            }
            assertEquals("1414770317901", record.get(0));
            assertEquals("android.widget.EditText", record.get(1));
            assertEquals("pass sem1 _84*|*", record.get(2));
            assertEquals("0", record.get(3));
            assertEquals("pass sem1 _8", record.get(4));
            assertEquals(5, record.size());
            // row 2
            record = parse(parser, failParseRecordNo);
            if (record == null) {
                return; // expected failure
            }
            assertEquals("1414770318470", record.get(0));
            assertEquals("android.widget.EditText", record.get(1));
            assertEquals("pass sem1 _84:|", record.get(2));
            assertEquals("0", record.get(3));
            assertEquals("pass sem1 _84:\\", record.get(4));
            assertEquals(5, record.size());
            // row 3: Fail for certain
            assertThrows(IOException.class, () -> parser.nextRecord());
        }
    }

    private void testCSV141Ok(final shaded.org.apache.commons.csv.CSVFormat format) throws IOException {
        final Path path = Paths.get("src/test/resources/shaded/org/apache/commons/csv/CSV-141/csv-141.csv");
        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse(path, StandardCharsets.UTF_8, format)) {
            // row 1
            shaded.org.apache.commons.csv.CSVRecord record = parser.nextRecord();
            assertEquals("1414770317901", record.get(0));
            assertEquals("android.widget.EditText", record.get(1));
            assertEquals("pass sem1 _84*|*", record.get(2));
            assertEquals("0", record.get(3));
            assertEquals("pass sem1 _8", record.get(4));
            assertEquals(5, record.size());
            // row 2
            record = parser.nextRecord();
            assertEquals("1414770318470", record.get(0));
            assertEquals("android.widget.EditText", record.get(1));
            assertEquals("pass sem1 _84:|", record.get(2));
            assertEquals("0", record.get(3));
            assertEquals("pass sem1 _84:\\", record.get(4));
            assertEquals(5, record.size());
            // row 3
            record = parser.nextRecord();
            assertEquals("1414770318327", record.get(0));
            assertEquals("android.widget.EditText", record.get(1));
            assertEquals("pass sem1", record.get(2));
            assertEquals(3, record.size());
            // row 4
            record = parser.nextRecord();
            assertEquals("1414770318628", record.get(0));
            assertEquals("android.widget.EditText", record.get(1));
            assertEquals("pass sem1 _84*|*", record.get(2));
            assertEquals("0", record.get(3));
            assertEquals("pass sem1", record.get(4));
            assertEquals(5, record.size());
        }
    }

    @Test
    public void testCSV235() throws IOException {
        final String dqString = "\"aaa\",\"b\"\"bb\",\"ccc\""; // "aaa","b""bb","ccc"
        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVFormat.RFC4180.parse(new StringReader(dqString))) {
            final Iterator<shaded.org.apache.commons.csv.CSVRecord> records = parser.iterator();
            final shaded.org.apache.commons.csv.CSVRecord record = records.next();
            assertFalse(records.hasNext());
            assertEquals(3, record.size());
            assertEquals("aaa", record.get(0));
            assertEquals("b\"bb", record.get(1));
            assertEquals("ccc", record.get(2));
        }
    }

    @Test
    public void testCSV57() throws Exception {
        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse("", shaded.org.apache.commons.csv.CSVFormat.DEFAULT)) {
            final List<shaded.org.apache.commons.csv.CSVRecord> list = parser.getRecords();
            assertNotNull(list);
            assertEquals(0, list.size());
        }
    }

    @Test
    public void testDefaultFormat() throws IOException {
        final String code = "a,b#\n" // 1)
                + "\"\n\",\" \",#\n" // 2)
                + "#,\"\"\n" // 3)
                + "# Final comment\n"// 4)
                ;
        final String[][] res = {{"a", "b#"}, {"\n", " ", "#"}, {"#", ""}, {"# Final comment"}};

        shaded.org.apache.commons.csv.CSVFormat format = shaded.org.apache.commons.csv.CSVFormat.DEFAULT;
        assertFalse(format.isCommentMarkerSet());
        final String[][] res_comments = {{"a", "b#"}, {"\n", " ", "#"},};

        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse(code, format)) {
            final List<shaded.org.apache.commons.csv.CSVRecord> records = parser.getRecords();
            assertFalse(records.isEmpty());

            Utils.compare("Failed to parse without comments", res, records);

            format = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withCommentMarker('#');
        }
        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse(code, format)) {
            final List<shaded.org.apache.commons.csv.CSVRecord> records = parser.getRecords();

            Utils.compare("Failed to parse with comments", res_comments, records);
        }
    }

    @Test
    public void testDuplicateHeadersAllowedByDefault() throws Exception {
        try (shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse("a,b,a\n1,2,3\nx,y,z", shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withHeader())) {
            // noop
        }
    }

    @Test
    public void testDuplicateHeadersNotAllowed() {
        assertThrows(IllegalArgumentException.class,
                () -> shaded.org.apache.commons.csv.CSVParser.parse("a,b,a\n1,2,3\nx,y,z", shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withHeader().withAllowDuplicateHeaderNames(false)));
    }

    @Test
    public void testEmptyFile() throws Exception {
        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse(Paths.get("src/test/resources/shaded/org/apache/commons/csv/empty.txt"), StandardCharsets.UTF_8,
                shaded.org.apache.commons.csv.CSVFormat.DEFAULT)) {
            assertNull(parser.nextRecord());
        }
    }

    @Test
    public void testEmptyFileHeaderParsing() throws Exception {
        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse("", shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withFirstRecordAsHeader())) {
            assertNull(parser.nextRecord());
            assertTrue(parser.getHeaderNames().isEmpty());
        }
    }

    @Test
    public void testEmptyLineBehaviorCSV() throws Exception {
        final String[] codes = {"hello,\r\n\r\n\r\n", "hello,\n\n\n", "hello,\"\"\r\n\r\n\r\n", "hello,\"\"\n\n\n"};
        final String[][] res = {{"hello", ""} // CSV format ignores empty lines
        };
        for (final String code : codes) {
            try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse(code, shaded.org.apache.commons.csv.CSVFormat.DEFAULT)) {
                final List<shaded.org.apache.commons.csv.CSVRecord> records = parser.getRecords();
                assertEquals(res.length, records.size());
                assertFalse(records.isEmpty());
                for (int i = 0; i < res.length; i++) {
                    assertArrayEquals(res[i], records.get(i).values());
                }
            }
        }
    }

    @Test
    public void testEmptyLineBehaviorExcel() throws Exception {
        final String[] codes = {"hello,\r\n\r\n\r\n", "hello,\n\n\n", "hello,\"\"\r\n\r\n\r\n", "hello,\"\"\n\n\n"};
        final String[][] res = {{"hello", ""}, {""}, // Excel format does not ignore empty lines
                {""}};
        for (final String code : codes) {
            try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse(code, shaded.org.apache.commons.csv.CSVFormat.EXCEL)) {
                final List<shaded.org.apache.commons.csv.CSVRecord> records = parser.getRecords();
                assertEquals(res.length, records.size());
                assertFalse(records.isEmpty());
                for (int i = 0; i < res.length; i++) {
                    assertArrayEquals(res[i], records.get(i).values());
                }
            }
        }
    }

    @Test
    public void testEmptyString() throws Exception {
        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse("", shaded.org.apache.commons.csv.CSVFormat.DEFAULT)) {
            assertNull(parser.nextRecord());
        }
    }

    @Test
    public void testEndOfFileBehaviorCSV() throws Exception {
        final String[] codes = {"hello,\r\n\r\nworld,\r\n", "hello,\r\n\r\nworld,", "hello,\r\n\r\nworld,\"\"\r\n", "hello,\r\n\r\nworld,\"\"",
                "hello,\r\n\r\nworld,\n", "hello,\r\n\r\nworld,", "hello,\r\n\r\nworld,\"\"\n", "hello,\r\n\r\nworld,\"\""};
        final String[][] res = {{"hello", ""}, // CSV format ignores empty lines
                {"world", ""}};
        for (final String code : codes) {
            try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse(code, shaded.org.apache.commons.csv.CSVFormat.DEFAULT)) {
                final List<shaded.org.apache.commons.csv.CSVRecord> records = parser.getRecords();
                assertEquals(res.length, records.size());
                assertFalse(records.isEmpty());
                for (int i = 0; i < res.length; i++) {
                    assertArrayEquals(res[i], records.get(i).values());
                }
            }
        }
    }

    @Test
    public void testEndOfFileBehaviorExcel() throws Exception {
        final String[] codes = {"hello,\r\n\r\nworld,\r\n", "hello,\r\n\r\nworld,", "hello,\r\n\r\nworld,\"\"\r\n", "hello,\r\n\r\nworld,\"\"",
                "hello,\r\n\r\nworld,\n", "hello,\r\n\r\nworld,", "hello,\r\n\r\nworld,\"\"\n", "hello,\r\n\r\nworld,\"\""};
        final String[][] res = {{"hello", ""}, {""}, // Excel format does not ignore empty lines
                {"world", ""}};

        for (final String code : codes) {
            try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse(code, shaded.org.apache.commons.csv.CSVFormat.EXCEL)) {
                final List<shaded.org.apache.commons.csv.CSVRecord> records = parser.getRecords();
                assertEquals(res.length, records.size());
                assertFalse(records.isEmpty());
                for (int i = 0; i < res.length; i++) {
                    assertArrayEquals(res[i], records.get(i).values());
                }
            }
        }
    }

    @Test
    public void testExcelFormat1() throws IOException {
        final String code = "value1,value2,value3,value4\r\na,b,c,d\r\n  x,,," + "\r\n\r\n\"\"\"hello\"\"\",\"  \"\"world\"\"\",\"abc\ndef\",\r\n";
        final String[][] res = {{"value1", "value2", "value3", "value4"}, {"a", "b", "c", "d"}, {"  x", "", "", ""}, {""},
                {"\"hello\"", "  \"world\"", "abc\ndef", ""}};
        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse(code, shaded.org.apache.commons.csv.CSVFormat.EXCEL)) {
            final List<shaded.org.apache.commons.csv.CSVRecord> records = parser.getRecords();
            assertEquals(res.length, records.size());
            assertFalse(records.isEmpty());
            for (int i = 0; i < res.length; i++) {
                assertArrayEquals(res[i], records.get(i).values());
            }
        }
    }

    @Test
    public void testExcelFormat2() throws Exception {
        final String code = "foo,baar\r\n\r\nhello,\r\n\r\nworld,\r\n";
        final String[][] res = {{"foo", "baar"}, {""}, {"hello", ""}, {""}, {"world", ""}};
        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse(code, shaded.org.apache.commons.csv.CSVFormat.EXCEL)) {
            final List<shaded.org.apache.commons.csv.CSVRecord> records = parser.getRecords();
            assertEquals(res.length, records.size());
            assertFalse(records.isEmpty());
            for (int i = 0; i < res.length; i++) {
                assertArrayEquals(res[i], records.get(i).values());
            }
        }
    }

    /**
     * Tests an exported Excel worksheet with a header row and rows that have more columns than the headers
     */
    @Test
    public void testExcelHeaderCountLessThanData() throws Exception {
        final String code = "A,B,C,,\r\na,b,c,d,e\r\n";
        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse(code, shaded.org.apache.commons.csv.CSVFormat.EXCEL.withHeader())) {
            parser.getRecords().forEach(record -> {
                assertEquals("a", record.get("A"));
                assertEquals("b", record.get("B"));
                assertEquals("c", record.get("C"));
            });
        }
    }

    @Test
    public void testFirstEndOfLineCr() throws IOException {
        final String data = "foo\rbaar,\rhello,world\r,kanu";
        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse(data, shaded.org.apache.commons.csv.CSVFormat.DEFAULT)) {
            final List<shaded.org.apache.commons.csv.CSVRecord> records = parser.getRecords();
            assertEquals(4, records.size());
            assertEquals("\r", parser.getFirstEndOfLine());
        }
    }

    @Test
    public void testFirstEndOfLineCrLf() throws IOException {
        final String data = "foo\r\nbaar,\r\nhello,world\r\n,kanu";
        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse(data, shaded.org.apache.commons.csv.CSVFormat.DEFAULT)) {
            final List<shaded.org.apache.commons.csv.CSVRecord> records = parser.getRecords();
            assertEquals(4, records.size());
            assertEquals("\r\n", parser.getFirstEndOfLine());
        }
    }

    @Test
    public void testFirstEndOfLineLf() throws IOException {
        final String data = "foo\nbaar,\nhello,world\n,kanu";
        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse(data, shaded.org.apache.commons.csv.CSVFormat.DEFAULT)) {
            final List<shaded.org.apache.commons.csv.CSVRecord> records = parser.getRecords();
            assertEquals(4, records.size());
            assertEquals("\n", parser.getFirstEndOfLine());
        }
    }

    @Test
    public void testForEach() throws Exception {
        try (final Reader in = new StringReader("a,b,c\n1,2,3\nx,y,z"); final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.parse(in)) {
            final List<shaded.org.apache.commons.csv.CSVRecord> records = new ArrayList<>();
            for (final shaded.org.apache.commons.csv.CSVRecord record : parser) {
                records.add(record);
            }
            assertEquals(3, records.size());
            assertArrayEquals(new String[]{"a", "b", "c"}, records.get(0).values());
            assertArrayEquals(new String[]{"1", "2", "3"}, records.get(1).values());
            assertArrayEquals(new String[]{"x", "y", "z"}, records.get(2).values());
        }
    }

    @Test
    public void testGetHeaderComment_HeaderComment1() throws IOException {
        try (shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse(CSV_INPUT_HEADER_COMMENT, FORMAT_AUTO_HEADER)) {
            parser.getRecords();
            // Expect a header comment
            assertTrue(parser.hasHeaderComment());
            assertEquals("header comment", parser.getHeaderComment());
        }
    }

    @Test
    public void testGetHeaderComment_HeaderComment2() throws IOException {
        try (shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse(CSV_INPUT_HEADER_COMMENT, FORMAT_EXPLICIT_HEADER)) {
            parser.getRecords();
            // Expect a header comment
            assertTrue(parser.hasHeaderComment());
            assertEquals("header comment", parser.getHeaderComment());
        }
    }

    @Test
    public void testGetHeaderComment_HeaderComment3() throws IOException {
        try (shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse(CSV_INPUT_HEADER_COMMENT, FORMAT_EXPLICIT_HEADER_NOSKIP)) {
            parser.getRecords();
            // Expect no header comment - the text "comment" is attached to the first record
            assertFalse(parser.hasHeaderComment());
            assertNull(parser.getHeaderComment());
        }
    }

    @Test
    public void testGetHeaderComment_HeaderTrailerComment() throws IOException {
        try (shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse(CSV_INPUT_MULTILINE_HEADER_TRAILER_COMMENT, FORMAT_AUTO_HEADER)) {
            parser.getRecords();
            // Expect a header comment
            assertTrue(parser.hasHeaderComment());
            assertEquals("multi-line" + LF + "header comment", parser.getHeaderComment());
        }
    }

    @Test
    public void testGetHeaderComment_NoComment1() throws IOException {
        try (shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse(CSV_INPUT_NO_COMMENT, FORMAT_AUTO_HEADER)) {
            parser.getRecords();
            // Expect no header comment
            assertFalse(parser.hasHeaderComment());
            assertNull(parser.getHeaderComment());
        }
    }

    @Test
    public void testGetHeaderComment_NoComment2() throws IOException {
        try (shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse(CSV_INPUT_NO_COMMENT, FORMAT_EXPLICIT_HEADER)) {
            parser.getRecords();
            // Expect no header comment
            assertFalse(parser.hasHeaderComment());
            assertNull(parser.getHeaderComment());
        }
    }

    @Test
    public void testGetHeaderComment_NoComment3() throws IOException {
        try (shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse(CSV_INPUT_NO_COMMENT, FORMAT_EXPLICIT_HEADER_NOSKIP)) {
            parser.getRecords();
            // Expect no header comment
            assertFalse(parser.hasHeaderComment());
            assertNull(parser.getHeaderComment());
        }
    }

    @Test
    public void testGetHeaderMap() throws Exception {
        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse("a,b,c\n1,2,3\nx,y,z", shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withHeader("A", "B", "C"))) {
            final Map<String, Integer> headerMap = parser.getHeaderMap();
            final Iterator<String> columnNames = headerMap.keySet().iterator();
            // Headers are iterated in column order.
            assertEquals("A", columnNames.next());
            assertEquals("B", columnNames.next());
            assertEquals("C", columnNames.next());
            final Iterator<shaded.org.apache.commons.csv.CSVRecord> records = parser.iterator();

            // Parse to make sure getHeaderMap did not have a side-effect.
            for (int i = 0; i < 3; i++) {
                assertTrue(records.hasNext());
                final shaded.org.apache.commons.csv.CSVRecord record = records.next();
                assertEquals(record.get(0), record.get("A"));
                assertEquals(record.get(1), record.get("B"));
                assertEquals(record.get(2), record.get("C"));
            }

            assertFalse(records.hasNext());
        }
    }

    @Test
    public void testGetHeaderNames() throws IOException {
        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse("a,b,c\n1,2,3\nx,y,z", shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withHeader("A", "B", "C"))) {
            final Map<String, Integer> nameIndexMap = parser.getHeaderMap();
            final List<String> headerNames = parser.getHeaderNames();
            assertNotNull(headerNames);
            assertEquals(nameIndexMap.size(), headerNames.size());
            for (int i = 0; i < headerNames.size(); i++) {
                final String name = headerNames.get(i);
                assertEquals(i, nameIndexMap.get(name).intValue());
            }
        }
    }

    @Test
    public void testGetHeaderNamesReadOnly() throws IOException {
        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse("a,b,c\n1,2,3\nx,y,z", shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withHeader("A", "B", "C"))) {
            final List<String> headerNames = parser.getHeaderNames();
            assertNotNull(headerNames);
            assertThrows(UnsupportedOperationException.class, () -> headerNames.add("This is a read-only list."));
        }
    }

    @Test
    public void testGetLine() throws IOException {
        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse(CSV_INPUT, shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withIgnoreSurroundingSpaces())) {
            for (final String[] re : RESULT) {
                assertArrayEquals(re, parser.nextRecord().values());
            }

            assertNull(parser.nextRecord());
        }
    }

    @Test
    public void testGetLineNumberWithCR() throws Exception {
        this.validateLineNumbers(String.valueOf(CR));
    }

    @Test
    public void testGetLineNumberWithCRLF() throws Exception {
        this.validateLineNumbers(CRLF);
    }

    @Test
    public void testGetLineNumberWithLF() throws Exception {
        this.validateLineNumbers(String.valueOf(LF));
    }

    @Test
    public void testGetOneLine() throws IOException {
        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse(CSV_INPUT_1, shaded.org.apache.commons.csv.CSVFormat.DEFAULT)) {
            final shaded.org.apache.commons.csv.CSVRecord record = parser.getRecords().get(0);
            assertArrayEquals(RESULT[0], record.values());
        }
    }

    /**
     * Tests reusing a parser to process new string records one at a time as they are being discovered. See [CSV-110].
     *
     * @throws IOException when an I/O error occurs.
     */
    @Test
    public void testGetOneLineOneParser() throws IOException {
        final shaded.org.apache.commons.csv.CSVFormat format = shaded.org.apache.commons.csv.CSVFormat.DEFAULT;
        try (final PipedWriter writer = new PipedWriter(); final shaded.org.apache.commons.csv.CSVParser parser = new shaded.org.apache.commons.csv.CSVParser(new PipedReader(writer), format)) {
            writer.append(CSV_INPUT_1);
            writer.append(format.getRecordSeparator());
            final shaded.org.apache.commons.csv.CSVRecord record1 = parser.nextRecord();
            assertArrayEquals(RESULT[0], record1.values());
            writer.append(CSV_INPUT_2);
            writer.append(format.getRecordSeparator());
            final shaded.org.apache.commons.csv.CSVRecord record2 = parser.nextRecord();
            assertArrayEquals(RESULT[1], record2.values());
        }
    }

    @Test
    public void testGetRecordNumberWithCR() throws Exception {
        this.validateRecordNumbers(String.valueOf(CR));
    }

    @Test
    public void testGetRecordNumberWithCRLF() throws Exception {
        this.validateRecordNumbers(CRLF);
    }

    @Test
    public void testGetRecordNumberWithLF() throws Exception {
        this.validateRecordNumbers(String.valueOf(LF));
    }

    @Test
    public void testGetRecordPositionWithCRLF() throws Exception {
        this.validateRecordPosition(CRLF);
    }

    @Test
    public void testGetRecordPositionWithLF() throws Exception {
        this.validateRecordPosition(String.valueOf(LF));
    }

    @Test
    public void testGetRecords() throws IOException {
        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse(CSV_INPUT, shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withIgnoreSurroundingSpaces())) {
            final List<shaded.org.apache.commons.csv.CSVRecord> records = parser.getRecords();
            assertEquals(RESULT.length, records.size());
            assertFalse(records.isEmpty());
            for (int i = 0; i < RESULT.length; i++) {
                assertArrayEquals(RESULT[i], records.get(i).values());
            }
        }
    }

    @Test
    public void testGetRecordsFromBrokenInputStream() throws IOException {
        @SuppressWarnings("resource") // We also get an exception on close, which is OK but can't assert in a try.
        final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse(new BrokenInputStream(), UTF_8, shaded.org.apache.commons.csv.CSVFormat.DEFAULT);
        assertThrows(UncheckedIOException.class, parser::getRecords);

    }

    @Test
    public void testGetRecordWithMultiLineValues() throws Exception {
        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse("\"a\r\n1\",\"a\r\n2\"" + CRLF + "\"b\r\n1\",\"b\r\n2\"" + CRLF + "\"c\r\n1\",\"c\r\n2\"",
                shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withRecordSeparator(CRLF))) {
            shaded.org.apache.commons.csv.CSVRecord record;
            assertEquals(0, parser.getRecordNumber());
            assertEquals(0, parser.getCurrentLineNumber());
            assertNotNull(record = parser.nextRecord());
            assertEquals(3, parser.getCurrentLineNumber());
            assertEquals(1, record.getRecordNumber());
            assertEquals(1, parser.getRecordNumber());
            assertNotNull(record = parser.nextRecord());
            assertEquals(6, parser.getCurrentLineNumber());
            assertEquals(2, record.getRecordNumber());
            assertEquals(2, parser.getRecordNumber());
            assertNotNull(record = parser.nextRecord());
            assertEquals(9, parser.getCurrentLineNumber());
            assertEquals(3, record.getRecordNumber());
            assertEquals(3, parser.getRecordNumber());
            assertNull(record = parser.nextRecord());
            assertEquals(9, parser.getCurrentLineNumber());
            assertEquals(3, parser.getRecordNumber());
        }
    }

    @Test
    public void testGetTrailerComment_HeaderComment1() throws IOException {
        try (shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse(CSV_INPUT_HEADER_COMMENT, FORMAT_AUTO_HEADER)) {
            parser.getRecords();
            assertFalse(parser.hasTrailerComment());
            assertNull(parser.getTrailerComment());
        }
    }

    @Test
    public void testGetTrailerComment_HeaderComment2() throws IOException {
        try (shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse(CSV_INPUT_HEADER_COMMENT, FORMAT_EXPLICIT_HEADER)) {
            parser.getRecords();
            assertFalse(parser.hasTrailerComment());
            assertNull(parser.getTrailerComment());
        }
    }

    @Test
    public void testGetTrailerComment_HeaderComment3() throws IOException {
        try (shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse(CSV_INPUT_HEADER_COMMENT, FORMAT_EXPLICIT_HEADER_NOSKIP)) {
            parser.getRecords();
            assertFalse(parser.hasTrailerComment());
            assertNull(parser.getTrailerComment());
        }
    }

    @Test
    public void testGetTrailerComment_HeaderTrailerComment1() throws IOException {
        try (shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse(CSV_INPUT_HEADER_TRAILER_COMMENT, FORMAT_AUTO_HEADER)) {
            parser.getRecords();
            assertTrue(parser.hasTrailerComment());
            assertEquals("comment", parser.getTrailerComment());
        }
    }

    @Test
    public void testGetTrailerComment_HeaderTrailerComment2() throws IOException {
        try (shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse(CSV_INPUT_HEADER_TRAILER_COMMENT, FORMAT_EXPLICIT_HEADER)) {
            parser.getRecords();
            assertTrue(parser.hasTrailerComment());
            assertEquals("comment", parser.getTrailerComment());
        }
    }

    @Test
    public void testGetTrailerComment_HeaderTrailerComment3() throws IOException {
        try (shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse(CSV_INPUT_HEADER_TRAILER_COMMENT, FORMAT_EXPLICIT_HEADER_NOSKIP)) {
            parser.getRecords();
            assertTrue(parser.hasTrailerComment());
            assertEquals("comment", parser.getTrailerComment());
        }
    }

    @Test
    public void testGetTrailerComment_MultilineComment() throws IOException {
        try (shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse(CSV_INPUT_MULTILINE_HEADER_TRAILER_COMMENT, FORMAT_AUTO_HEADER)) {
            parser.getRecords();
            assertTrue(parser.hasTrailerComment());
            assertEquals("multi-line" + LF + "comment", parser.getTrailerComment());
        }
    }

    @Test
    public void testHeader() throws Exception {
        final Reader in = new StringReader("a,b,c\n1,2,3\nx,y,z");

        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withHeader().parse(in)) {
            final Iterator<shaded.org.apache.commons.csv.CSVRecord> records = parser.iterator();

            for (int i = 0; i < 2; i++) {
                assertTrue(records.hasNext());
                final shaded.org.apache.commons.csv.CSVRecord record = records.next();
                assertEquals(record.get(0), record.get("a"));
                assertEquals(record.get(1), record.get("b"));
                assertEquals(record.get(2), record.get("c"));
            }

            assertFalse(records.hasNext());
        }
    }

    @Test
    public void testHeaderComment() throws Exception {
        final Reader in = new StringReader("# comment\na,b,c\n1,2,3\nx,y,z");

        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withCommentMarker('#').withHeader().parse(in)) {
            final Iterator<shaded.org.apache.commons.csv.CSVRecord> records = parser.iterator();

            for (int i = 0; i < 2; i++) {
                assertTrue(records.hasNext());
                final shaded.org.apache.commons.csv.CSVRecord record = records.next();
                assertEquals(record.get(0), record.get("a"));
                assertEquals(record.get(1), record.get("b"));
                assertEquals(record.get(2), record.get("c"));
            }

            assertFalse(records.hasNext());
        }
    }

    @Test
    public void testHeaderMissing() throws Exception {
        final Reader in = new StringReader("a,,c\n1,2,3\nx,y,z");

        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withHeader().withAllowMissingColumnNames().parse(in)) {
            final Iterator<shaded.org.apache.commons.csv.CSVRecord> records = parser.iterator();

            for (int i = 0; i < 2; i++) {
                assertTrue(records.hasNext());
                final shaded.org.apache.commons.csv.CSVRecord record = records.next();
                assertEquals(record.get(0), record.get("a"));
                assertEquals(record.get(2), record.get("c"));
            }

            assertFalse(records.hasNext());
        }
    }

    @Test
    public void testHeaderMissingWithNull() throws Exception {
        final Reader in = new StringReader("a,,c,,e\n1,2,3,4,5\nv,w,x,y,z");
        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withHeader().withNullString("").withAllowMissingColumnNames().parse(in)) {
            parser.iterator();
        }
    }

    @Test
    public void testHeadersMissing() throws Exception {
        try (final Reader in = new StringReader("a,,c,,e\n1,2,3,4,5\nv,w,x,y,z");
             final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withHeader().withAllowMissingColumnNames().parse(in)) {
            parser.iterator();
        }
    }

    @Test
    public void testHeadersMissingException() {
        final Reader in = new StringReader("a,,c,,e\n1,2,3,4,5\nv,w,x,y,z");
        assertThrows(IllegalArgumentException.class, () -> shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withHeader().parse(in).iterator());
    }

    @Test
    public void testHeadersMissingOneColumnException() {
        final Reader in = new StringReader("a,,c,d,e\n1,2,3,4,5\nv,w,x,y,z");
        assertThrows(IllegalArgumentException.class, () -> shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withHeader().parse(in).iterator());
    }

    @Test
    public void testHeadersWithNullColumnName() throws IOException {
        final Reader in = new StringReader("header1,null,header3\n1,2,3\n4,5,6");
        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withHeader().withNullString("null").withAllowMissingColumnNames().parse(in)) {
            final Iterator<shaded.org.apache.commons.csv.CSVRecord> records = parser.iterator();
            final shaded.org.apache.commons.csv.CSVRecord record = records.next();
            // Expect the null header to be missing
            @SuppressWarnings("resource") final shaded.org.apache.commons.csv.CSVParser recordParser = record.getParser();
            assertEquals(Arrays.asList("header1", "header3"), recordParser.getHeaderNames());
            assertEquals(2, recordParser.getHeaderMap().size());
        }
    }

    @Test
    public void testIgnoreCaseHeaderMapping() throws Exception {
        final Reader reader = new StringReader("1,2,3");
        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withHeader("One", "TWO", "three").withIgnoreHeaderCase().parse(reader)) {
            final Iterator<shaded.org.apache.commons.csv.CSVRecord> records = parser.iterator();
            final shaded.org.apache.commons.csv.CSVRecord record = records.next();
            assertEquals("1", record.get("one"));
            assertEquals("2", record.get("two"));
            assertEquals("3", record.get("THREE"));
        }
    }

    @Test
    public void testIgnoreEmptyLines() throws IOException {
        final String code = "\nfoo,baar\n\r\n,\n\n,world\r\n\n";
        // String code = "world\r\n\n";
        // String code = "foo;baar\r\n\r\nhello;\r\n\r\nworld;\r\n";
        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse(code, shaded.org.apache.commons.csv.CSVFormat.DEFAULT)) {
            final List<shaded.org.apache.commons.csv.CSVRecord> records = parser.getRecords();
            assertEquals(3, records.size());
        }
    }

    @Test
    public void testInvalidFormat() {
        assertThrows(IllegalArgumentException.class, () -> shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withDelimiter(CR));
    }

    @Test
    public void testIterator() throws Exception {
        final Reader in = new StringReader("a,b,c\n1,2,3\nx,y,z");

        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.parse(in)) {
            final Iterator<shaded.org.apache.commons.csv.CSVRecord> iterator = parser.iterator();

            assertTrue(iterator.hasNext());
            assertThrows(UnsupportedOperationException.class, iterator::remove);
            assertArrayEquals(new String[]{"a", "b", "c"}, iterator.next().values());
            assertArrayEquals(new String[]{"1", "2", "3"}, iterator.next().values());
            assertTrue(iterator.hasNext());
            assertTrue(iterator.hasNext());
            assertTrue(iterator.hasNext());
            assertArrayEquals(new String[]{"x", "y", "z"}, iterator.next().values());
            assertFalse(iterator.hasNext());

            assertThrows(NoSuchElementException.class, iterator::next);
        }
    }

    @Test
    public void testIteratorSequenceBreaking() throws IOException {
        final String fiveRows = "1\n2\n3\n4\n5\n";

        // Iterator hasNext() shouldn't break sequence
        try (shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.parse(new StringReader(fiveRows))) {

            final Iterator<shaded.org.apache.commons.csv.CSVRecord> iter = parser.iterator();
            int recordNumber = 0;
            while (iter.hasNext()) {
                final shaded.org.apache.commons.csv.CSVRecord record = iter.next();
                recordNumber++;
                assertEquals(String.valueOf(recordNumber), record.get(0));
                if (recordNumber >= 2) {
                    break;
                }
            }
            iter.hasNext();
            while (iter.hasNext()) {
                final shaded.org.apache.commons.csv.CSVRecord record = iter.next();
                recordNumber++;
                assertEquals(String.valueOf(recordNumber), record.get(0));
            }
        }

        // Consecutive enhanced for loops shouldn't break sequence
        try (shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.parse(new StringReader(fiveRows))) {
            int recordNumber = 0;
            for (final shaded.org.apache.commons.csv.CSVRecord record : parser) {
                recordNumber++;
                assertEquals(String.valueOf(recordNumber), record.get(0));
                if (recordNumber >= 2) {
                    break;
                }
            }
            for (final shaded.org.apache.commons.csv.CSVRecord record : parser) {
                recordNumber++;
                assertEquals(String.valueOf(recordNumber), record.get(0));
            }
        }

        // Consecutive enhanced for loops with hasNext() peeking shouldn't break sequence
        try (shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.parse(new StringReader(fiveRows))) {
            int recordNumber = 0;
            for (final shaded.org.apache.commons.csv.CSVRecord record : parser) {
                recordNumber++;
                assertEquals(String.valueOf(recordNumber), record.get(0));
                if (recordNumber >= 2) {
                    break;
                }
            }
            parser.iterator().hasNext();
            for (final shaded.org.apache.commons.csv.CSVRecord record : parser) {
                recordNumber++;
                assertEquals(String.valueOf(recordNumber), record.get(0));
            }
        }
    }

    @Test
    public void testLineFeedEndings() throws IOException {
        final String code = "foo\nbaar,\nhello,world\n,kanu";
        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse(code, shaded.org.apache.commons.csv.CSVFormat.DEFAULT)) {
            final List<shaded.org.apache.commons.csv.CSVRecord> records = parser.getRecords();
            assertEquals(4, records.size());
        }
    }

    @Test
    public void parseAndRaiseException() throws Exception {
        final Reader in = new StringReader("a,b,c\n1,2\nx,y,z");
        CSVFormat.Builder builder = CSVFormat.Builder.create();
        CSVFormat format = builder.setDelimiter("`").setSkipHeaderRecord(true).setFieldCount(3).setValidateFieldCount(true).setHeader("A", "B", "C").build();
        try (final shaded.org.apache.commons.csv.CSVParser parser = format.parse(in)) {
            final Iterator<shaded.org.apache.commons.csv.CSVRecord> records = parser.iterator();
            Assertions.assertThrows(FieldCountMismatchException.class, () -> {
                records.next();
            });
        }
    }

    @Test
    public void testMappedButNotSetAsOutlook2007ContactExport() throws Exception {
        final Reader in = new StringReader("a,b,c\n1,2\nx,y,z");
        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withHeader("A", "B", "C").withSkipHeaderRecord().parse(in)) {
            final Iterator<shaded.org.apache.commons.csv.CSVRecord> records = parser.iterator();
            shaded.org.apache.commons.csv.CSVRecord record;

            // 1st record
            record = records.next();
            assertTrue(record.isMapped("A"));
            assertTrue(record.isMapped("B"));
            assertTrue(record.isMapped("C"));
            assertTrue(record.isSet("A"));
            assertTrue(record.isSet("B"));
            assertFalse(record.isSet("C"));
            assertEquals("1", record.get("A"));
            assertEquals("2", record.get("B"));
            assertFalse(record.isConsistent());

            // 2nd record
            record = records.next();
            assertTrue(record.isMapped("A"));
            assertTrue(record.isMapped("B"));
            assertTrue(record.isMapped("C"));
            assertTrue(record.isSet("A"));
            assertTrue(record.isSet("B"));
            assertTrue(record.isSet("C"));
            assertEquals("x", record.get("A"));
            assertEquals("y", record.get("B"));
            assertEquals("z", record.get("C"));
            assertTrue(record.isConsistent());

            assertFalse(records.hasNext());
        }
    }

    @Test
    // TODO this may lead to strange behavior, throw an exception if iterator() has already been called?
    public void testMultipleIterators() throws Exception {
        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse("a,b,c" + CRLF + "d,e,f", shaded.org.apache.commons.csv.CSVFormat.DEFAULT)) {
            final Iterator<shaded.org.apache.commons.csv.CSVRecord> itr1 = parser.iterator();

            final shaded.org.apache.commons.csv.CSVRecord first = itr1.next();
            assertEquals("a", first.get(0));
            assertEquals("b", first.get(1));
            assertEquals("c", first.get(2));

            final shaded.org.apache.commons.csv.CSVRecord second = itr1.next();
            assertEquals("d", second.get(0));
            assertEquals("e", second.get(1));
            assertEquals("f", second.get(2));
        }
    }

    @Test
    public void testNewCSVParserNullReaderFormat() {
        assertThrows(NullPointerException.class, () -> new shaded.org.apache.commons.csv.CSVParser(null, shaded.org.apache.commons.csv.CSVFormat.DEFAULT));
    }

    @Test
    public void testNewCSVParserReaderNullFormat() {
        assertThrows(NullPointerException.class, () -> new shaded.org.apache.commons.csv.CSVParser(new StringReader(""), null));
    }

    @Test
    public void testNoHeaderMap() throws Exception {
        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse("a,b,c\n1,2,3\nx,y,z", shaded.org.apache.commons.csv.CSVFormat.DEFAULT)) {
            assertNull(parser.getHeaderMap());
        }
    }

    @Test
    public void testNotValueCSV() throws IOException {
        final String source = "#";
        final shaded.org.apache.commons.csv.CSVFormat csvFormat = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withCommentMarker('#');
        try (final shaded.org.apache.commons.csv.CSVParser csvParser = csvFormat.parse(new StringReader(source))) {
            final shaded.org.apache.commons.csv.CSVRecord csvRecord = csvParser.nextRecord();
            assertNull(csvRecord);
        }
    }

    @Test
    public void testParse() throws Exception {
        final ClassLoader loader = ClassLoader.getSystemClassLoader();
        final URL url = loader.getResource("shaded/org/apache/commons/csv/CSVFileParser/test.csv");
        final shaded.org.apache.commons.csv.CSVFormat format = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withHeader("A", "B", "C", "D");
        final Charset charset = StandardCharsets.UTF_8;

        try (@SuppressWarnings("resource") // CSVParser closes the input resource
             final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse(new InputStreamReader(url.openStream(), charset), format)) {
            parseFully(parser);
        }
        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse(new String(Files.readAllBytes(Paths.get(url.toURI())), charset), format)) {
            parseFully(parser);
        }
        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse(new File(url.toURI()), charset, format)) {
            parseFully(parser);
        }
        try (@SuppressWarnings("resource") // CSVParser closes the input resource
             final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse(url.openStream(), charset, format)) {
            parseFully(parser);
        }
        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse(Paths.get(url.toURI()), charset, format)) {
            parseFully(parser);
        }
        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse(url, charset, format)) {
            parseFully(parser);
        }
        try (final shaded.org.apache.commons.csv.CSVParser parser = new shaded.org.apache.commons.csv.CSVParser(new InputStreamReader(url.openStream(), charset), format)) {
            parseFully(parser);
        }
        try (final shaded.org.apache.commons.csv.CSVParser parser = new shaded.org.apache.commons.csv.CSVParser(new InputStreamReader(url.openStream(), charset), format, /* characterOffset= */0, /* recordNumber= */1)) {
            parseFully(parser);
        }
    }

    @Test
    public void testParseFileNullFormat() {
        assertThrows(NullPointerException.class, () -> shaded.org.apache.commons.csv.CSVParser.parse(new File("CSVFileParser/test.csv"), Charset.defaultCharset(), null));
    }

    @Test
    public void testParseNullFileFormat() {
        assertThrows(NullPointerException.class, () -> shaded.org.apache.commons.csv.CSVParser.parse((File) null, Charset.defaultCharset(), shaded.org.apache.commons.csv.CSVFormat.DEFAULT));
    }

    @Test
    public void testParseNullPathFormat() {
        assertThrows(NullPointerException.class, () -> shaded.org.apache.commons.csv.CSVParser.parse((Path) null, Charset.defaultCharset(), shaded.org.apache.commons.csv.CSVFormat.DEFAULT));
    }

    @Test
    public void testParseNullStringFormat() {
        assertThrows(NullPointerException.class, () -> shaded.org.apache.commons.csv.CSVParser.parse((String) null, shaded.org.apache.commons.csv.CSVFormat.DEFAULT));
    }

    @Test
    public void testParseNullUrlCharsetFormat() {
        assertThrows(NullPointerException.class, () -> shaded.org.apache.commons.csv.CSVParser.parse((URL) null, Charset.defaultCharset(), shaded.org.apache.commons.csv.CSVFormat.DEFAULT));
    }

    @Test
    public void testParserUrlNullCharsetFormat() {
        assertThrows(NullPointerException.class, () -> shaded.org.apache.commons.csv.CSVParser.parse(new URL("https://commons.apache.org"), null, shaded.org.apache.commons.csv.CSVFormat.DEFAULT));
    }

    @Test
    public void testParseStringNullFormat() {
        assertThrows(NullPointerException.class, () -> shaded.org.apache.commons.csv.CSVParser.parse("csv data", null));
    }

    @Test
    public void testParseUrlCharsetNullFormat() {
        assertThrows(NullPointerException.class, () -> shaded.org.apache.commons.csv.CSVParser.parse(new URL("https://commons.apache.org"), Charset.defaultCharset(), null));
    }

    @Test
    public void testParseWithDelimiterStringWithEscape() throws IOException {
        final String source = "a![!|!]b![|]c[|]xyz\r\nabc[abc][|]xyz";
        final shaded.org.apache.commons.csv.CSVFormat csvFormat = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.builder().setDelimiter("[|]").setEscape('!').build();
        try (shaded.org.apache.commons.csv.CSVParser csvParser = csvFormat.parse(new StringReader(source))) {
            shaded.org.apache.commons.csv.CSVRecord csvRecord = csvParser.nextRecord();
            assertEquals("a[|]b![|]c", csvRecord.get(0));
            assertEquals("xyz", csvRecord.get(1));
            csvRecord = csvParser.nextRecord();
            assertEquals("abc[abc]", csvRecord.get(0));
            assertEquals("xyz", csvRecord.get(1));
        }
    }

    @Test
    public void testParseWithDelimiterStringWithQuote() throws IOException {
        final String source = "'a[|]b[|]c'[|]xyz\r\nabc[abc][|]xyz";
        final shaded.org.apache.commons.csv.CSVFormat csvFormat = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.builder().setDelimiter("[|]").setQuote('\'').build();
        try (shaded.org.apache.commons.csv.CSVParser csvParser = csvFormat.parse(new StringReader(source))) {
            shaded.org.apache.commons.csv.CSVRecord csvRecord = csvParser.nextRecord();
            assertEquals("a[|]b[|]c", csvRecord.get(0));
            assertEquals("xyz", csvRecord.get(1));
            csvRecord = csvParser.nextRecord();
            assertEquals("abc[abc]", csvRecord.get(0));
            assertEquals("xyz", csvRecord.get(1));
        }
    }

    @Test
    public void testParseWithDelimiterWithEscape() throws IOException {
        final String source = "a!,b!,c,xyz";
        final shaded.org.apache.commons.csv.CSVFormat csvFormat = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withEscape('!');
        try (shaded.org.apache.commons.csv.CSVParser csvParser = csvFormat.parse(new StringReader(source))) {
            final shaded.org.apache.commons.csv.CSVRecord csvRecord = csvParser.nextRecord();
            assertEquals("a,b,c", csvRecord.get(0));
            assertEquals("xyz", csvRecord.get(1));
        }
    }

    @Test
    public void testParseWithDelimiterWithQuote() throws IOException {
        final String source = "'a,b,c',xyz";
        final shaded.org.apache.commons.csv.CSVFormat csvFormat = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withQuote('\'');
        try (shaded.org.apache.commons.csv.CSVParser csvParser = csvFormat.parse(new StringReader(source))) {
            final shaded.org.apache.commons.csv.CSVRecord csvRecord = csvParser.nextRecord();
            assertEquals("a,b,c", csvRecord.get(0));
            assertEquals("xyz", csvRecord.get(1));
        }
    }

    @Test
    public void testParseWithQuoteThrowsException() {
        final shaded.org.apache.commons.csv.CSVFormat csvFormat = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withQuote('\'');
        assertThrows(IOException.class, () -> csvFormat.parse(new StringReader("'a,b,c','")).nextRecord());
        assertThrows(IOException.class, () -> csvFormat.parse(new StringReader("'a,b,c'abc,xyz")).nextRecord());
        assertThrows(IOException.class, () -> csvFormat.parse(new StringReader("'abc'a,b,c',xyz")).nextRecord());
    }

    @Test
    public void testParseWithQuoteWithEscape() throws IOException {
        final String source = "'a?,b?,c?d',xyz";
        final shaded.org.apache.commons.csv.CSVFormat csvFormat = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withQuote('\'').withEscape('?');
        try (shaded.org.apache.commons.csv.CSVParser csvParser = csvFormat.parse(new StringReader(source))) {
            final shaded.org.apache.commons.csv.CSVRecord csvRecord = csvParser.nextRecord();
            assertEquals("a,b,c?d", csvRecord.get(0));
            assertEquals("xyz", csvRecord.get(1));
        }
    }

    @Test
    public void testProvidedHeader() throws Exception {
        final Reader in = new StringReader("a,b,c\n1,2,3\nx,y,z");

        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withHeader("A", "B", "C").parse(in)) {
            final Iterator<shaded.org.apache.commons.csv.CSVRecord> records = parser.iterator();

            for (int i = 0; i < 3; i++) {
                assertTrue(records.hasNext());
                final shaded.org.apache.commons.csv.CSVRecord record = records.next();
                assertTrue(record.isMapped("A"));
                assertTrue(record.isMapped("B"));
                assertTrue(record.isMapped("C"));
                assertFalse(record.isMapped("NOT MAPPED"));
                assertEquals(record.get(0), record.get("A"));
                assertEquals(record.get(1), record.get("B"));
                assertEquals(record.get(2), record.get("C"));
            }

            assertFalse(records.hasNext());
        }
    }

    @Test
    public void testProvidedHeaderAuto() throws Exception {
        final Reader in = new StringReader("a,b,c\n1,2,3\nx,y,z");

        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withHeader().parse(in)) {
            final Iterator<shaded.org.apache.commons.csv.CSVRecord> records = parser.iterator();

            for (int i = 0; i < 2; i++) {
                assertTrue(records.hasNext());
                final shaded.org.apache.commons.csv.CSVRecord record = records.next();
                assertTrue(record.isMapped("a"));
                assertTrue(record.isMapped("b"));
                assertTrue(record.isMapped("c"));
                assertFalse(record.isMapped("NOT MAPPED"));
                assertEquals(record.get(0), record.get("a"));
                assertEquals(record.get(1), record.get("b"));
                assertEquals(record.get(2), record.get("c"));
            }

            assertFalse(records.hasNext());
        }
    }

    @Test
    public void testRepeatedHeadersAreReturnedInCSVRecordHeaderNames() throws IOException {
        final Reader in = new StringReader("header1,header2,header1\n1,2,3\n4,5,6");
        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withFirstRecordAsHeader().withTrim().parse(in)) {
            final Iterator<shaded.org.apache.commons.csv.CSVRecord> records = parser.iterator();
            final shaded.org.apache.commons.csv.CSVRecord record = records.next();
            @SuppressWarnings("resource") final shaded.org.apache.commons.csv.CSVParser recordParser = record.getParser();
            assertEquals(Arrays.asList("header1", "header2", "header1"), recordParser.getHeaderNames());
        }
    }

    @Test
    public void testRoundtrip() throws Exception {
        final StringWriter out = new StringWriter();
        final String data = "a,b,c\r\n1,2,3\r\nx,y,z\r\n";
        try (final shaded.org.apache.commons.csv.CSVPrinter printer = new CSVPrinter(out, shaded.org.apache.commons.csv.CSVFormat.DEFAULT);
             final shaded.org.apache.commons.csv.CSVParser parse = shaded.org.apache.commons.csv.CSVParser.parse(data, shaded.org.apache.commons.csv.CSVFormat.DEFAULT)) {
            for (final shaded.org.apache.commons.csv.CSVRecord record : parse) {
                printer.printRecord(record);
            }
            assertEquals(data, out.toString());
        }
    }

    @Test
    public void testSkipAutoHeader() throws Exception {
        final Reader in = new StringReader("a,b,c\n1,2,3\nx,y,z");
        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withHeader().parse(in)) {
            final Iterator<shaded.org.apache.commons.csv.CSVRecord> records = parser.iterator();
            final shaded.org.apache.commons.csv.CSVRecord record = records.next();
            assertEquals("1", record.get("a"));
            assertEquals("2", record.get("b"));
            assertEquals("3", record.get("c"));
        }
    }

    @Test
    public void testSkipHeaderOverrideDuplicateHeaders() throws Exception {
        final Reader in = new StringReader("a,a,a\n1,2,3\nx,y,z");
        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withHeader("X", "Y", "Z").withSkipHeaderRecord().parse(in)) {
            final Iterator<shaded.org.apache.commons.csv.CSVRecord> records = parser.iterator();
            final shaded.org.apache.commons.csv.CSVRecord record = records.next();
            assertEquals("1", record.get("X"));
            assertEquals("2", record.get("Y"));
            assertEquals("3", record.get("Z"));
        }
    }

    @Test
    public void testSkipSetAltHeaders() throws Exception {
        final Reader in = new StringReader("a,b,c\n1,2,3\nx,y,z");
        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withHeader("X", "Y", "Z").withSkipHeaderRecord().parse(in)) {
            final Iterator<shaded.org.apache.commons.csv.CSVRecord> records = parser.iterator();
            final shaded.org.apache.commons.csv.CSVRecord record = records.next();
            assertEquals("1", record.get("X"));
            assertEquals("2", record.get("Y"));
            assertEquals("3", record.get("Z"));
        }
    }

    @Test
    public void testSkipSetHeader() throws Exception {
        final Reader in = new StringReader("a,b,c\n1,2,3\nx,y,z");
        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withHeader("a", "b", "c").withSkipHeaderRecord().parse(in)) {
            final Iterator<shaded.org.apache.commons.csv.CSVRecord> records = parser.iterator();
            final shaded.org.apache.commons.csv.CSVRecord record = records.next();
            assertEquals("1", record.get("a"));
            assertEquals("2", record.get("b"));
            assertEquals("3", record.get("c"));
        }
    }

    @Test
    public void testStream() throws Exception {
        final Reader in = new StringReader("a,b,c\n1,2,3\nx,y,z");
        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.parse(in)) {
            final List<shaded.org.apache.commons.csv.CSVRecord> list = parser.stream().collect(Collectors.toList());
            assertFalse(list.isEmpty());
            assertArrayEquals(new String[]{"a", "b", "c"}, list.get(0).values());
            assertArrayEquals(new String[]{"1", "2", "3"}, list.get(1).values());
            assertArrayEquals(new String[]{"x", "y", "z"}, list.get(2).values());
        }
    }

    @Test
    public void testTrailingDelimiter() throws Exception {
        final Reader in = new StringReader("a,a,a,\n\"1\",\"2\",\"3\",\nx,y,z,");
        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withHeader("X", "Y", "Z").withSkipHeaderRecord().withTrailingDelimiter().parse(in)) {
            final Iterator<shaded.org.apache.commons.csv.CSVRecord> records = parser.iterator();
            final shaded.org.apache.commons.csv.CSVRecord record = records.next();
            assertEquals("1", record.get("X"));
            assertEquals("2", record.get("Y"));
            assertEquals("3", record.get("Z"));
            assertEquals(3, record.size());
        }
    }

    @Test
    public void testTrim() throws Exception {
        final Reader in = new StringReader("a,a,a\n\" 1 \",\" 2 \",\" 3 \"\nx,y,z");
        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withHeader("X", "Y", "Z").withSkipHeaderRecord().withTrim().parse(in)) {
            final Iterator<shaded.org.apache.commons.csv.CSVRecord> records = parser.iterator();
            final shaded.org.apache.commons.csv.CSVRecord record = records.next();
            assertEquals("1", record.get("X"));
            assertEquals("2", record.get("Y"));
            assertEquals("3", record.get("Z"));
            assertEquals(3, record.size());
        }
    }

    private void validateLineNumbers(final String lineSeparator) throws IOException {
        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse("a" + lineSeparator + "b" + lineSeparator + "c", shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withRecordSeparator(lineSeparator))) {
            assertEquals(0, parser.getCurrentLineNumber());
            assertNotNull(parser.nextRecord());
            assertEquals(1, parser.getCurrentLineNumber());
            assertNotNull(parser.nextRecord());
            assertEquals(2, parser.getCurrentLineNumber());
            assertNotNull(parser.nextRecord());
            // Read EOF without EOL should 3
            assertEquals(3, parser.getCurrentLineNumber());
            assertNull(parser.nextRecord());
            // Read EOF without EOL should 3
            assertEquals(3, parser.getCurrentLineNumber());
        }
    }

    private void validateRecordNumbers(final String lineSeparator) throws IOException {
        try (final shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse("a" + lineSeparator + "b" + lineSeparator + "c", shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withRecordSeparator(lineSeparator))) {
            shaded.org.apache.commons.csv.CSVRecord record;
            assertEquals(0, parser.getRecordNumber());
            assertNotNull(record = parser.nextRecord());
            assertEquals(1, record.getRecordNumber());
            assertEquals(1, parser.getRecordNumber());
            assertNotNull(record = parser.nextRecord());
            assertEquals(2, record.getRecordNumber());
            assertEquals(2, parser.getRecordNumber());
            assertNotNull(record = parser.nextRecord());
            assertEquals(3, record.getRecordNumber());
            assertEquals(3, parser.getRecordNumber());
            assertNull(record = parser.nextRecord());
            assertEquals(3, parser.getRecordNumber());
        }
    }

    private void validateRecordPosition(final String lineSeparator) throws IOException {
        final String nl = lineSeparator; // used as linebreak in values for better distinction

        final String code = "a,b,c" + lineSeparator + "1,2,3" + lineSeparator +
                // to see if recordPosition correctly points to the enclosing quote
                "'A" + nl + "A','B" + nl + "B',CC" + lineSeparator +
                // unicode test... not very relevant while operating on strings instead of bytes, but for
                // completeness...
                "\u00c4,\u00d6,\u00dc" + lineSeparator + "EOF,EOF,EOF";

        final shaded.org.apache.commons.csv.CSVFormat format = CSVFormat.newFormat(',').withQuote('\'').withRecordSeparator(lineSeparator);
        shaded.org.apache.commons.csv.CSVParser parser = shaded.org.apache.commons.csv.CSVParser.parse(code, format);

        CSVRecord record;
        assertEquals(0, parser.getRecordNumber());

        assertNotNull(record = parser.nextRecord());
        assertEquals(1, record.getRecordNumber());
        assertEquals(code.indexOf('a'), record.getCharacterPosition());

        assertNotNull(record = parser.nextRecord());
        assertEquals(2, record.getRecordNumber());
        assertEquals(code.indexOf('1'), record.getCharacterPosition());

        assertNotNull(record = parser.nextRecord());
        final long positionRecord3 = record.getCharacterPosition();
        assertEquals(3, record.getRecordNumber());
        assertEquals(code.indexOf("'A"), record.getCharacterPosition());
        assertEquals("A" + lineSeparator + "A", record.get(0));
        assertEquals("B" + lineSeparator + "B", record.get(1));
        assertEquals("CC", record.get(2));

        assertNotNull(record = parser.nextRecord());
        assertEquals(4, record.getRecordNumber());
        assertEquals(code.indexOf('\u00c4'), record.getCharacterPosition());

        assertNotNull(record = parser.nextRecord());
        assertEquals(5, record.getRecordNumber());
        assertEquals(code.indexOf("EOF"), record.getCharacterPosition());

        parser.close();

        // now try to read starting at record 3
        parser = new CSVParser(new StringReader(code.substring((int) positionRecord3)), format, positionRecord3, 3);

        assertNotNull(record = parser.nextRecord());
        assertEquals(3, record.getRecordNumber());
        assertEquals(code.indexOf("'A"), record.getCharacterPosition());
        assertEquals("A" + lineSeparator + "A", record.get(0));
        assertEquals("B" + lineSeparator + "B", record.get(1));
        assertEquals("CC", record.get(2));

        assertNotNull(record = parser.nextRecord());
        assertEquals(4, record.getRecordNumber());
        assertEquals(code.indexOf('\u00c4'), record.getCharacterPosition());
        assertEquals("\u00c4", record.get(0));

        parser.close();
    }

}
