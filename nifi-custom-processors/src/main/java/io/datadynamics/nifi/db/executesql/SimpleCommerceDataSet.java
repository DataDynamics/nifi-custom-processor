package io.datadynamics.nifi.db.executesql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

/**
 * A sample data set for test consists of 'persons', 'products' and 'relationships' tables.
 */
public class SimpleCommerceDataSet {

    static String dropPersons = "drop table persons";
    static String dropProducts = "drop table products";
    static String dropRelationships = "drop table relationships";
    static String createPersons = "create table persons (id integer, name varchar(100), code integer)";
    static String createProducts = "create table products (id integer, name varchar(100), code integer)";
    static String createRelationships = "create table relationships (id integer,name varchar(100), code integer)";
    static Random rng = new Random(53495);

    public static void loadTestData2Database(Connection con, int nrOfPersons, int nrOfProducts, int nrOfRels) throws SQLException {

        System.out.println(createRandomName());
        System.out.println(createRandomName());
        System.out.println(createRandomName());

        final Statement st = con.createStatement();

        // tables may not exist, this is not serious problem.
        try {
            st.executeUpdate(dropPersons);
        } catch (final Exception ignored) {
        }

        try {
            st.executeUpdate(dropProducts);
        } catch (final Exception ignored) {
        }

        try {
            st.executeUpdate(dropRelationships);
        } catch (final Exception ignored) {
        }

        st.executeUpdate(createPersons);
        st.executeUpdate(createProducts);
        st.executeUpdate(createRelationships);

        for (int i = 0; i < nrOfPersons; i++)
            loadPersons(st, i);

        for (int i = 0; i < nrOfProducts; i++)
            loadProducts(st, i);

        for (int i = 0; i < nrOfRels; i++)
            loadRelationships(st, i);

        st.close();
    }

    static private void loadPersons(Statement st, int nr) throws SQLException {
        st.executeUpdate("insert into persons values (" + nr + ", '" + createRandomName() + "', " + rng.nextInt(469946) + ")");
    }

    static private void loadProducts(Statement st, int nr) throws SQLException {
        st.executeUpdate("insert into products values (" + nr + ", '" + createRandomName() + "', " + rng.nextInt(469946) + ")");
    }

    static private void loadRelationships(Statement st, int nr) throws SQLException {
        st.executeUpdate("insert into relationships values (" + nr + ", '" + createRandomName() + "', " + rng.nextInt(469946) + ")");
    }

    static private String createRandomName() {
        return createRandomString() + " " + createRandomString();
    }

    static private String createRandomString() {

        final int length = rng.nextInt(10);
        final String characters = "ABCDEFGHIJ";

        final char[] text = new char[length];
        for (int i = 0; i < length; i++) {
            text[i] = characters.charAt(rng.nextInt(characters.length()));
        }
        return new String(text);
    }

    private Connection createConnection(String location) throws ClassNotFoundException, SQLException {
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
        return DriverManager.getConnection("jdbc:derby:" + location + ";create=true");
    }

}