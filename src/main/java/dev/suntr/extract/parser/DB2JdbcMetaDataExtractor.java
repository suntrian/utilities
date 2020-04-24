package dev.suntr.extract.parser;

import dev.suntr.extract.entity.RawRoutine;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class DB2JdbcMetaDataExtractor extends DefaultJdbcMetaDataExtractor {

  private final static Set<String> SYS_CAT = new HashSet<>(Arrays.asList(
          "administrator",
          "nullid",
          "sqlj",
          "syscat",
          "sysfun",
          "sysibm",
          "sysibmadm",
          "sysibminternal",
          "sysibmts",
          "sysproc",
          "syspublic",
          "sysstat",
          "systools"));

  private DB2JdbcMetaDataExtractor(){}

  public static JdbcMetaDataExtractor newInstance(){
    return new DB2JdbcMetaDataExtractor();
  }

  @Override
  protected boolean isSysCatalog(String catalogName) {
    return catalogName!=null && SYS_CAT.contains(catalogName.toLowerCase());
  }

  @Override
  protected boolean isSysSchema(String schemaName) {
    return schemaName!=null && SYS_CAT.contains(schemaName.toLowerCase());
  }

  @Override
  protected boolean isSysTable(String tableName) {
    return false;
  }

  @Override
  protected ResultSet decideDatabase(DatabaseMetaData metaData) throws SQLException {
    return metaData.getSchemas();
  }

  PreparedStatement pstmt = null;
  PreparedStatement tearDown = null;
  Map<String, Long> opTokenCache = new ConcurrentHashMap<>();

  @Override
  public String extractDDL(Connection connection, String ddlType, String catalog, String schema, String tableName) throws SQLException {
    if (!opTokenCache.containsKey(schema)){
      CallableStatement cstmt = connection.prepareCall("CALL SYSPROC.DB2LK_GENERATE_DDL(?, ?)");
      cstmt.setString(1, "-e -z " + schema);
      cstmt.registerOutParameter(2, Types.BIGINT);
      cstmt.executeUpdate();
      Long opToken = cstmt.getLong(2);
      opTokenCache.put(schema, opToken);
      tearDown = connection.prepareStatement("call SYSPROC.DB2LK_CLEAN_TABLE(?)");
    }
    Long opToken = opTokenCache.get(schema);
    if (pstmt == null || pstmt.isClosed()){
      pstmt = connection.prepareStatement("SELECT SQL_STMT FROM SYSTOOLS.DB2LOOK_INFO WHERE OP_TOKEN = ? AND OBJ_TYPE IN('TABLE','COLUMN','PKEY') AND (OBJ_NAME = ? OR OBJ_NAME LIKE ?)");
    }
    pstmt.setLong(1, opToken);
    pstmt.setString(2, tableName);
    pstmt.setString(3, tableName + "\".\"%");
    //String sql = "SELECT DISTINCT OBJ_TYPE, OBJ_NAME, OP_TOKEN FROM SYSTOOLS.DB2LOOK_INFO WHERE OP_TOKEN=:opToken AND OBJ_SCHEMA=:schemaName " ;
    ResultSet resultSet =  pstmt.executeQuery();
    StringBuilder builder = new StringBuilder();
    while (resultSet.next()){
      builder.append(resultSet.getString(1));
    }
    return builder.toString();
  }

  @Override
  public boolean supportDdl() {
    return true;
  }

  private final String ROUTINE_SQL = "SELECT " +
          "ROUTINE_CATALOG as ROUTINE_CAT, " +
          "ROUTINE_SCHEMA as ROUTINE_SCHEMA, " +
          "ROUTINE_NAME as ROUTINE_NAME, " +
          "ROUTINE_TYPE as ROUTINE_TYPE, " +
          "ROUTINE_DEFINITION as ROUTINE_DEFINITION, " +
          "NULL as ROUTINE_PARAMS, " +
          "NULL as ROUTINE_RETURNS, " +
          "CREATED as CREATED, " +
          "LAST_ALTERED as UPDATED " +
          "FROM SYSIBM.ROUTINES WHERE ROUTINE_SCHEMA = '%s'";

  @Override
  public List<RawRoutine> extractRoutine(Connection connection, String type, String catalog, String schema) throws SQLException {
    String sql = String.format(ROUTINE_SQL, schema);
    if (type!=null && !"".equals(type)){
      sql += " AND ROUTINE_TYPE = '" + type + "'";
    }
    PreparedStatement stmt = connection.prepareStatement(sql);
    ResultSet resultSet = stmt.executeQuery();
    return parseToRawRoutine(resultSet);
  }

    /**
     * @param connection
     * @param sql
     * @return
     * @throws SQLException
     * throw SQLException IF NOT EXISTS TABLE 'DB2INST1.EXPLAIN_INSTANCE'
     * create table EXPLAIN_INSTANCE by execute SQL '''
     * ### CALL SYSPROC.SYSINSTALLOBJECTS('EXPLAIN', 'C', CAST (NULL AS VARCHAR(128)), CAST (NULL AS VARCHAR(128)));
     * OR use db2 client by
     * ###  db2 -tf EXPLAIN.DDL
     */
  @Override
  public List<Map<String, Object>> explain(Connection connection, String sql) throws SQLException {
/*    String explainSql = "EXPLAIN PLAN FOR " + sql;
    Statement explain = connection.createStatement();
    explain.execute(explainSql);
    explain.close();
    String retrieveSql = "SELECT * FROM EXPLAIN_STATEMENT";
    try {
      return extractMap(connection, retrieveSql);
    } catch (SQLException e) {
      retrieveSql = "SELECT * FROM SYSTOOLS.EXPLAIN_STATEMENT";
      return extractMap(connection, retrieveSql);
    }*/
    throw new UnsupportedOperationException("db2默认不支持explain 查询");
  }

  @Override
  public boolean supportRoutine() {
    return true;
  }

  @Override
  public void close() {
    if (this.tearDown!=null){
      this.opTokenCache.values().forEach(o->{
        try {
          tearDown.setLong(1, o);
          tearDown.execute();
        } catch (SQLException e) {}
      });
    }
    super.close();
  }
}
