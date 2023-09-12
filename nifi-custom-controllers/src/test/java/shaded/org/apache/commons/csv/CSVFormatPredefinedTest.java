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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests {@link org.apache.commons.csv.CSVFormat.Predefined}.
 */
public class CSVFormatPredefinedTest {

    private void test(final shaded.org.apache.commons.csv.CSVFormat format, final String enumName) {
        assertEquals(format, shaded.org.apache.commons.csv.CSVFormat.Predefined.valueOf(enumName).getFormat());
        assertEquals(format, shaded.org.apache.commons.csv.CSVFormat.valueOf(enumName));
    }

    @Test
    public void testDefault() {
        test(shaded.org.apache.commons.csv.CSVFormat.DEFAULT, "Default");
    }

    @Test
    public void testExcel() {
        test(shaded.org.apache.commons.csv.CSVFormat.EXCEL, "Excel");
    }

    @Test
    public void testMongoDbCsv() {
        test(shaded.org.apache.commons.csv.CSVFormat.MONGODB_CSV, "MongoDBCsv");
    }

    @Test
    public void testMongoDbTsv() {
        test(shaded.org.apache.commons.csv.CSVFormat.MONGODB_TSV, "MongoDBTsv");
    }

    @Test
    public void testMySQL() {
        test(shaded.org.apache.commons.csv.CSVFormat.MYSQL, "MySQL");
    }

    @Test
    public void testOracle() {
        test(shaded.org.apache.commons.csv.CSVFormat.ORACLE, "Oracle");
    }

    @Test
    public void testPostgreSqlCsv() {
        test(shaded.org.apache.commons.csv.CSVFormat.POSTGRESQL_CSV, "PostgreSQLCsv");
    }

    @Test
    public void testPostgreSqlText() {
        test(shaded.org.apache.commons.csv.CSVFormat.POSTGRESQL_TEXT, "PostgreSQLText");
    }

    @Test
    public void testRFC4180() {
        test(shaded.org.apache.commons.csv.CSVFormat.RFC4180, "RFC4180");
    }

    @Test
    public void testTDF() {
        test(CSVFormat.TDF, "TDF");
    }
}
