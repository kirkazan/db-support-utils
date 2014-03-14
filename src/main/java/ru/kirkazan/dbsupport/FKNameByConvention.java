package ru.kirkazan.dbsupport;

import org.apache.commons.cli.*;

import java.sql.*;
import java.util.*;

/**
 * @author esadykov
 * @since 11.03.14 23:41
 */
public class FKNameByConvention
{
    private static boolean overrideExistingFK = false;
    private static boolean showSQL = false;

    public static void main(String[] args)
    {
        parseArgs(args);

        Connection connection = getConnection(args);
        if (connection == null) return;

        Statement statement = null;
        Statement dropStatement = null;
        Statement createStatement = null;
        Statement selectStatement = null;
        ResultSet resultSet = null;
        try
        {
            statement = connection.createStatement();
            if (showSQL) System.out.println(QUERY);
            resultSet = statement.executeQuery(QUERY);
            dropStatement = connection.createStatement();
            createStatement = connection.createStatement();
            selectStatement = connection.createStatement();
            while (resultSet.next())
            {
                String selQuery = resultSet.getString("sel");
                if (showSQL) System.out.println(selQuery);
                List<Map<String, Object>> list = getResultSetAsList(selectStatement.executeQuery(selQuery));

                String drpQuery = resultSet.getString("drp");
                System.out.println("Try drop fk " + resultSet.getString("conname"));
                if (showSQL) System.out.println(drpQuery);
                //dropStatement.execute(drpQuery);
                if (list.size() > 1)
                {
                    System.out.println("FK already exists: " + resultSet.getString("newname"));
                    //todo check indexes identity, drop if identical
                }
                else
                {
                    String crtQuery = resultSet.getString("crt");
                    System.out.println("Try create constraint " + resultSet.getString("newname"));
                    if (showSQL) System.out.println(crtQuery);
                    //createStatement.execute(crtQuery);
                }
                System.out.println("Try: commit");
                connection.commit();

            }
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
        finally
        {

            try
            {
                if (resultSet != null)
                    resultSet.close();
                if (statement != null)
                    statement.close();
                if (dropStatement != null)
                    dropStatement.close();
                if (createStatement != null)
                    createStatement.close();
                if (selectStatement != null)
                    selectStatement.close();
                connection.close();
            }
            catch (SQLException e)
            {
                e.printStackTrace();
            }
        }
    }

    private static CommandLine cl;

    private static Connection getConnection(String[] args)
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

        overrideExistingFK = cl.hasOption("override");
        showSQL = cl.hasOption("sql");
    }

    private static List<Map<String, Object>> getResultSetAsList(ResultSet rs, String... excludes) throws SQLException
    {
        List<Map<String, Object>> list = new ArrayList<Map<String,Object>>();
        ResultSetMetaData metaData = rs.getMetaData();
        while (rs.next())
        {
            int columnCount = metaData.getColumnCount();
            Map<String, Object> map = new HashMap<String, Object>(columnCount);
            for (int i = 1 ; i <= columnCount; i++)
            {
                map.put(metaData.getColumnName(i), rs.getObject(i));
            }
            list.add(map);
        }
        return list;
    }

    private static final String CONVENTION_NAME = "((select relname from pg_class where oid = conrelid) || '__' || \n" +
            "    (select case when array_length(conkey, 1) = 1 then (select attname from pg_attribute where attrelid = conrelid and attnum = any(conkey)) else '' end) || \n" +
            "    '_fk')";
    private static final String QUERY =
            "select\n" +
            "  *,\n" +
            "  'select * from pg_constraint where conname in (''' || " + CONVENTION_NAME+ " || ''', ''' || conname || ''');' sel,\n" +
            "  'ALTER TABLE ' || (select relname from pg_class where oid = conrelid) || ' DROP CONSTRAINT ' || conname || ';' drp,\n" +
            "  'ALTER TABLE ' || (select relname from pg_class where oid = conrelid) || \n" +
            "  ' ADD CONSTRAINT ' || " + CONVENTION_NAME +  " || \n" +
            "  ' FOREIGN KEY (' || array_to_string(array(select attname from pg_attribute where attrelid = conrelid and attnum = any(conkey)), ',') || ')' ||\n" +
            "  ' REFERENCES ' || (select relname from pg_class where oid = confrelid) || '(' || array_to_string(array(select attname from pg_attribute where attrelid = confrelid and attnum = any(confkey)),',') ||')' || \n" +
            "  ' MATCH ' || (select case when confmatchtype = 's' then 'simple' when confmatchtype = 'u' then 'simple' when confmatchtype = 'f' then 'full'  when confmatchtype = 'p' then 'partial' end) || \n" +
            "    ' ON UPDATE ' || (select case when confupdtype = 'a' then 'no action' when confupdtype = 'r' then 'restrict' when confupdtype = 'c' then 'cascade' when confupdtype = 'n' then 'set null' when confupdtype = 'd' then 'set default' end) ||\n" +
            "    ' ON DELETE ' || (select case when confupdtype = 'a' then 'no action' when confupdtype = 'r' then 'restrict' when confupdtype = 'c' then 'cascade' when confupdtype = 'n' then 'set null' when confupdtype = 'd' then 'set default' end) || ';' crt,\n" +
            "  " + CONVENTION_NAME + " newname\n" +
            "from pg_constraint c where contype = 'f' and conname != " + CONVENTION_NAME;

    private static Options options = new Options();
    static
    {
        Option url = new Option("url", true, "database JDBC url, ex: jdbc:postgresql://<host>:<port>/<dbname>");
        url.setRequired(true);
        options.addOption(url);
        options.addOption("username",true, "database username");
        options.addOption("password",true, "database password");
        options.addOption("override", false, "override existing foreign keys");
        options.addOption("sql", false, "show sql");
    }

}
