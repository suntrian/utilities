package dev.suntr.extract.datasource;

import java.sql.*;
import java.util.Objects;
import java.util.Properties;

public class DatasourceResolver {

  public static boolean checkConnectionValid(String url, String user, String password){
    Connection connection = null;
    try {
      connection = getConnection(url, user, password);
      return  connection != null;
    } catch (Exception e) {
      return false;
    } finally {
      close(connection);
    }
  }

  public static Connection getConnection(DriverType driver, String host, Integer port, String database, String user, String password) throws SQLException, ClassNotFoundException {
    return getConnection(DriverType.getUrl(driver, host, port, database), user, password);
  }

  public static Connection getConnection(String url, String user, String password) throws ClassNotFoundException, SQLException {
    Class.forName(DriverType.judgeDriver(url).getDriver());
    Properties properties = new Properties();
    properties.setProperty("user", user);
    properties.setProperty("password",password);
    //used for Mysql
    properties.setProperty("serverTimezone","UTC");
    //used for mysql to fetch table remarks
    properties.setProperty("useInformationSchema", "true");
    //used for Oracle
    properties.setProperty("remarks", "true");
    return DriverManager.getConnection(url, properties);
  }

  public static DatabaseMetaData getDatabaseMetaData(Connection connection) throws SQLException {
    assert connection!=null;
    return connection.getMetaData();
  }

  public static Statement statement(Connection connection) throws SQLException {
    assert Objects.nonNull(connection);
    return connection.createStatement();
  }

  public static PreparedStatement statement(Connection connection, String sql, Object... args) throws SQLException {
    assert Objects.nonNull(connection);
    PreparedStatement stmt = connection.prepareStatement(sql);
    for (int i = 0; i < args.length; i++){
      stmt.setObject(i+1, args[i]);
    }
    return stmt;
  }

  public static void close(ResultSet resultSet){
    if (resultSet!=null){
      try {
        resultSet.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }

  public static void close(Statement statement){
    if (statement!=null){
      try {
        statement.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }

  public static void close(Connection connection){
    if (connection!=null ){
      try {
        connection.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }

}
