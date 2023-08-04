package io.datadynamics.nifi.db;

import java.sql.SQLException;

/**
 * This is a marker class to distinguish a table not being found from other SQL exceptions
 */
public class TableNotFoundException extends SQLException {
    public TableNotFoundException(String s) {
        super(s);
    }
}