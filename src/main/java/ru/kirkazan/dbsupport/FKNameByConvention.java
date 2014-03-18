package ru.kirkazan.dbsupport;

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.text.MessageFormat;
import java.util.*;

/**
 * @author esadykov
 * @since 11.03.14 23:41
 */
public class FKNameByConvention
{
    private static Logger logger = LoggerFactory.getLogger("main");
    private static boolean dropDuplicated = false;
    private static boolean showSQL = false;
    private static Connection connection;
    private static Map<String, FK> fks;
    private static Map<String, FK> dependencies = new HashMap<String, FK>();
    private static int renamed = 0;
    private static int dropped = 0;
    private static int untouched = 0;
    private static int waiting = 0;

    public static void main(String[] args)
    {
        parseArgs(args);

        connection = getConnection();
        if (connection == null) return;

        Statement statement = null;
        try
        {
            statement = connection.createStatement();
            if (showSQL) logger.info(FKS_QUERY);
            ResultSet resultSet = statement.executeQuery(FKS_QUERY);
            fks = getResultSetAsMap(resultSet);
            resultSet.close();


            for (FK fk : fks.values())
            {
                process(fk);
            }
        }
        catch (SQLException e)
        {
            logger.error("Error", e);
        }
        finally
        {

            try
            {
                if (statement != null)
                    statement.close();
                connection.close();
            }
            catch (SQLException e)
            {
                logger.error("Error at finally", e);
            }
        }

        logger.info("");
        logger.info("TOTAL:");
        logger.info("    Untouched: {}", untouched);
        logger.info("    Renamed: {}", renamed);
        if (dropDuplicated)
            logger.info("    Dropped: {}", dropped);
        else
            logger.info("    To drop: {}", dropped);
        logger.info("    Wainting: {}", waiting - dropped);

    }

    private static void process(FK fk) throws SQLException
    {

        logger.info("fk {}", fk.getId());

        if (fk.isConvenientName())
        {
            logger.info("    already has convenient name", fk.name);
            untouched++;
            if (dependencies.containsKey(fk.name))
            {
                FK dfk = dependencies.get(fk.name);
                logger.info("    dependent fk {} should be dropped as duplicated, compare:", dfk.getId());
                logger.info("        name\t\t\t{}\t{}", fk.name, dfk.name);
                logger.info("        from table\t\t{}\t{}", fk.fromTable, dfk.fromTable);
                logger.info("        from column\t{}\t{}", fk.fromColumn, dfk.fromColumn);
                logger.info("        to table\t\t{}\t{}", fk.toTable, dfk.toTable);
                logger.info("        to column\t\t{}\t{}", fk.toColumn, dfk.toColumn);
                logger.info("        match type\t\t{}\t{}", fk.matchType, dfk.matchType);
                logger.info("        on update\t\t{}\t{}", fk.onUpdate, dfk.onUpdate);
                logger.info("        on create\t\t{}\t{}", fk.onDelete, dfk.onDelete);

                if (dropDuplicated)
                {
                    logger.info("    fk {} will be dropped", dfk.getId());
                    drop(dfk);
                    logger.info("    try to commit");
                    connection.commit();
                }
                else
                {
                    logger.info("    use -dropDuplicated for auto drop or use next ddl:");
                    logger.info("    {}", MessageFormat.format(DROP_CONSTRAINT_QUERY_TEMPLATE, dfk.fromTable, dfk.name));
                }
                dropped++;
            }
            return;
        }

        String convenientName = fk.getConvenientName();
        if (fks.containsKey(fk.getConvenientId()))
        {
            logger.info("    will be renamed after {}", convenientName);
            dependencies.put(convenientName, fk);
            waiting++;
            return;
        }

        logger.info("    will be dropped");
        drop(fk);

        logger.info("    will be added with name {}", convenientName);
        addWithConvenientName(fk);

        renamed++;

        logger.info("    try to commit");
        connection.commit();
    }

    public static final String ADD_CONSTRAINT_QUERY_TEMPLATE =
            "ALTER TABLE {0} ADD CONSTRAINT {1} FOREIGN KEY ({2}) REFERENCES {3} ({4}) " +
                    "MATCH {5} ON UPDATE {6} ON DELETE {7};";

    private static void addWithConvenientName(FK fk)  throws SQLException
    {
        Statement addStatement = null;
        try
        {
            addStatement = connection.createStatement();
            String query = MessageFormat.format(ADD_CONSTRAINT_QUERY_TEMPLATE,
                    fk.fromTable,
                    fk.getConvenientName(),
                    fk.fromColumn,
                    fk.toTable,
                    fk.toColumn,
                    fk.matchType,
                    fk.onUpdate,
                    fk.onDelete
            );
            if (showSQL)
                logger.debug("    sql: {}",query);
            addStatement.execute(query);
        }
        finally
        {
            if (addStatement != null)
                addStatement.close();
        }
    }

    public static final String DROP_CONSTRAINT_QUERY_TEMPLATE = "ALTER TABLE {0} DROP CONSTRAINT {1};";

    private static void drop(FK fk) throws SQLException
    {
        Statement dropStatement = null;
        try
        {
            dropStatement = connection.createStatement();
            String query = MessageFormat.format(DROP_CONSTRAINT_QUERY_TEMPLATE, fk.fromTable, fk.name);
            if (showSQL)
                logger.debug("    sql: {}",query);
            dropStatement.execute(query);
        }
        finally
        {
            if (dropStatement != null)
                dropStatement.close();
        }
    }

    private static CommandLine cl;

    private static Connection getConnection()
    {
        String url = cl.getOptionValue("url");
        String username = cl.getOptionValue("username");
        String password = cl.getOptionValue("password");
        try
        {
            final Connection connection = DriverManager.getConnection(url, username, password);
            connection.setAutoCommit(false);
            return connection;
        }
        catch (SQLException e)
        {
            System.out.println(e.getMessage());
            HelpFormatter hf = new HelpFormatter();
            hf.printHelp(FKNameByConvention.class.getName(), options);
            return null;
        }
    }

    private static void parseArgs(String[] args)
    {
        Parser parser = new GnuParser();
        try
        {
            cl = parser.parse(options, args);
        }
        catch (ParseException e)
        {
            System.out.println(e.getMessage());
            HelpFormatter hf = new HelpFormatter();
            hf.printHelp(FKNameByConvention.class.getName(), options);
        }

        dropDuplicated = cl.hasOption("-dropDuplicated");
        showSQL = cl.hasOption("sql");
    }

    private static Map<String, FK> getResultSetAsMap(ResultSet rs) throws SQLException
    {
        HashMap<String, FK> map = new HashMap<String, FK>();
        while (rs.next())
        {
            FK fk = getResultSetRowAsFK(rs);
            map.put(fk.getId(), fk);
        }
        return map;
    }

    private static FK getResultSetRowAsFK(ResultSet rs) throws SQLException
    {
        return new FK(
                rs.getString("fk_name"),
                rs.getString("from_table_name"),
                rs.getString("from_column_name"),
                rs.getString("to_table_name"),
                rs.getString("to_column_name"),
                POSTGRES_MATCH_TYPE_MAP.get(rs.getString("match_type")),
                POSTGRES_ACTION_MAP.get(rs.getString("update_type")),
                POSTGRES_ACTION_MAP.get(rs.getString("delete_type"))
        );
    }

    private static final Map<String, String> POSTGRES_ACTION_MAP = new HashMap<String, String>(5);

    static
    {
        POSTGRES_ACTION_MAP.put("a", "NO ACTION");
        POSTGRES_ACTION_MAP.put("r", "RESTRICT");
        POSTGRES_ACTION_MAP.put("c", "CASCADE");
        POSTGRES_ACTION_MAP.put("n", "SET NULL");
        POSTGRES_ACTION_MAP.put("d", "SET DEFAULT");
    }

    private static final Map<String, String> POSTGRES_MATCH_TYPE_MAP = new HashMap<String, String>(3);

    static
    {
        POSTGRES_MATCH_TYPE_MAP.put("f", "FULL");
        POSTGRES_MATCH_TYPE_MAP.put("p", "PARTIAL");
        POSTGRES_MATCH_TYPE_MAP.put("s", "SIMPLE");
    }

    private static final String FKS_QUERY = "" +
            "select\n" +
            "  conname fk_name,\n" +
            "  from_table.relname from_table_name,\n" +
            "  from_column.attname from_column_name,\n" +
            "  to_table.relname to_table_name,\n" +
            "  to_column.attname to_column_name,\n" +
            "  confmatchtype match_type,\n" +
            "  confupdtype update_type,\n" +
            "  confdeltype delete_type\n" +
            "from pg_catalog.pg_constraint\n" +
            "join pg_catalog.pg_class from_table on from_table.oid = conrelid\n" +
            "join pg_catalog.pg_class to_table on to_table.oid = confrelid\n" +
            "join pg_catalog.pg_attribute from_column on from_column.attrelid = conrelid and from_column.attnum = any(conkey)\n" +
            "join pg_catalog.pg_attribute to_column on to_column.attrelid = confrelid and to_column.attnum = any(confkey)\n" +
            "where \n" +
            "  contype = 'f'";

    private static Options options = new Options();

    static
    {
        Option url = new Option("url", true, "database JDBC url, ex: jdbc:postgresql://<host>:<port>/<dbname>");
        url.setRequired(true);
        options.addOption(url);
        options.addOption("username", true, "database username");
        options.addOption("password", true, "database password");
        options.addOption("dropDuplicated", false, "drop duplicated foreign keys");
        options.addOption("sql", false, "show sql");
    }
}
