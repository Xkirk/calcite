package src;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.commons.dbcp.BasicDataSource;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

/**
 * Created by kirk on 2017/7/4.
 */
public class Calcite {

    public static void main(String[] args) throws Exception{

        String presto_on_hadoop= "jdbc:presto://192.168.7.56:8055/hive/default";
        String presto_on_mesos= "jdbc:presto://192.168.7.8:8055";
        Properties info = new Properties();
        info.setProperty("user", "xujing");
        Class.forName("org.apache.calcite.jdbc.Driver");
        Connection connection = DriverManager.getConnection(presto_on_hadoop, info);
        CalciteConnection calciteConnection =
                connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
//        Class.forName("com.mysql.jdbc.Driver");
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setUrl(presto_on_hadoop);
        dataSource.setUsername("xujing");
//        dataSource.setPassword("password");
        Schema schema = JdbcSchema.create(rootSchema, "default", dataSource,
                null, "name");
        rootSchema.add("default", schema);
        Statement statement = calciteConnection.createStatement();
        ResultSet resultSet = statement.executeQuery(
//                "select d.deptno, min(e.empid)\n"
//                        + "from hr.emps as e\n"
//                        + "join hr.depts as d\n"
//                        + "  on e.deptno = d.deptno\n"
//                        + "group by d.deptno\n"
//                        + "having count(*) > 1");
        "select hour,count(*) from logdata where dt='20170705' group by hour order by hour");
        ResultSetMetaData meta = resultSet.getMetaData();
        int columnCount = meta.getColumnCount();
        while (resultSet.next()) {
            for (int i = 1; i < columnCount+1; i++) {
                System.out.print((meta.getColumnName(i)+":"+resultSet.getString(i)));
            }
            System.out.println();
        }
        resultSet.close();
        statement.close();
        connection.close();

//        Class.forName("com.mysql.jdbc.Driver");
//        BasicDataSource dataSource = new BasicDataSource();
//        dataSource.setUrl("jdbc:mysql://localhost");
//        dataSource.setUsername("username");
//        dataSource.setPassword("password");
//        SchemaPlus rootSchema = calciteConnection.getRootSchema();
//        Schema schema = JdbcSchema.create(rootSchema, "hr", dataSource,
//                null, "name");
    }
}