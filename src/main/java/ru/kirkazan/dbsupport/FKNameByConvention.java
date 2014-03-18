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
    private static Set<String> added = new HashSet<String>();
    private static Set<String> dropped = new HashSet<String>();

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
                process(fk, 1);
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
    }

    private static void process(FK fk, int recursionLevel) throws SQLException
    {

        String indent = getIndent(recursionLevel);
        logger.info("{}fk {}", getIndent(recursionLevel - 1), fk.getId());

        if (fk.isConvenientName())
        {
            logger.info("{}already has convenient name", indent, fk.name);
            if (dependencies.containsKey(fk.name))
            {
                FK dfk = dependencies.get(fk.name);
                logger.info("{}dependent fk {} should be dropped as duplicated, compare:", indent, dfk.getId());
                String indentPlus = getIndent(recursionLevel + 1);
                logger.info("{}name\t\t\t{}\t{}", indentPlus, fk.name, dfk.name);
                logger.info("{}from table\t{}\t{}", indentPlus, fk.fromTable, dfk.fromTable);
                logger.info("{}from column\t{}\t{}", indentPlus, fk.fromColumn, dfk.fromColumn);
                logger.info("{}to table\t\t{}\t{}", indentPlus, fk.toTable, dfk.toTable);
                logger.info("{}to column\t\t{}\t{}", indentPlus, fk.toColumn, dfk.toColumn);
                logger.info("{}match type\t{}\t{}", indentPlus, fk.matchType, dfk.matchType);
                logger.info("{}on update\t\t{}\t{}", indentPlus, fk.onUpdate, dfk.onUpdate);
                logger.info("{}on create\t\t{}\t{}", indentPlus, fk.onDelete, dfk.onDelete);

                if (dropDuplicated)
                {
                    logger.info("{}fk {} will be dropped",  indentPlus, dfk.getId());
                    drop(dfk);
                    dropped.add(dfk.name);
                    added.remove(dfk.name);
                    logger.info("{}possible dependencies", indentPlus);
                    if (dependencies.containsKey(dfk.getConvenientName()))
                        process(dependencies.get(dfk.getConvenientName()), recursionLevel + 1);
                }
                else
                {
                    logger.info("{}use -dropDuplicated for auto drop or use next ddl:", indentPlus);
                    logger.info("{}{}",indentPlus, MessageFormat.format(DROP_CONSTRAINT_QUERY_TEMPLATE, dfk.fromTable, dfk.name));
                }
            }
            return;
        }

        String convenientName = fk.getConvenientName();
        if ((fks.containsKey(fk.getConvenientId()) && !dropped.contains(convenientName)) || added.contains(convenientName))
        {
            logger.info("{}will be renamed after {}", indent, convenientName);
            dependencies.put(convenientName, fk);
            return;
        }

        logger.info("{}will be dropped",  indent);
        drop(fk);
        dropped.add(fk.name);
        added.remove(fk.name);

        logger.info("{}will be added with name {}", indent, convenientName);
        addWithConvenientName(fk);
        added.add(convenientName);
        dropped.remove(convenientName);

        logger.info("{}try to commit", indent);
        connection.commit();

        if (dependencies.containsKey(fk.name))
        {
            FK dfk = dependencies.get(fk.name);
            logger.info("{}dependent fk {} will be renamed", indent, dfk.name);
            process(dfk, recursionLevel + 1);
        }


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
                logger.debug(query);
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
                logger.debug(query);
            dropStatement.execute(query);
        }
        finally
        {
            if (dropStatement != null)
                dropStatement.close();
        }
    }

    public static final String ONE_LEVEL_INDENT = "    ";
    private static String getIndent(int recursionLevel)
    {
        StringBuilder indent = new StringBuilder((recursionLevel + 1) * ONE_LEVEL_INDENT.length());

        for (int i = 0 ; i < recursionLevel; i++)
        {
            indent.append(ONE_LEVEL_INDENT);
        }
        return indent.toString();
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
