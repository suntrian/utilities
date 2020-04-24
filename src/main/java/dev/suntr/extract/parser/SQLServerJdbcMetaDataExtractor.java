package dev.suntr.extract.parser;

import com.sun.istack.internal.Nullable;
import dev.suntr.extract.datasource.DatasourceResolver;
import dev.suntr.extract.entity.RawColumn;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

@SuppressWarnings("Duplicates")
public final class SQLServerJdbcMetaDataExtractor extends DefaultJdbcMetaDataExtractor {

    private static final Set<String> SYS_CAT = new HashSet<>(Arrays.asList(
            "master",
            "model",
            "msdb",
            "tempdb",
            "northwind",
            "pubs",
            "reportserver",  //"ReportServer",
            "reportservertempdb" //"ReportServerTempDB"
    ));

    private static final Set<String> SYS_SCHEMA = new HashSet<>(Arrays.asList(
            //"dbo",
            "guest",
            "information_schema",
            "sys",
            "db_owner",
            "db_accessadmin",
            "db_securityadmin",
            "db_ddladmin",
            "db_backupoperator",
            "db_datareader",
            "db_datawriter",
            "db_denydatareader",
            "db_denydatawriter"
    ));

    private static final Set<String> SYS_TABLE = new HashSet<>(Arrays.asList(
            "SYSDIAGRAMS",
            "KEY_COLUMN_USAGE",
            "REFERENTIAL_CONSTRAINTS",
            "PARAMETERS",
            "VIEW_TABLE_USAGE",
            "COLUMN_DOMAIN_USAGE",
            "TABLE_CONSTRAINTS",
            "VIEWS",
            "DOMAIN_CONSTRAINTS",
            "ROUTINE_COLUMNS",
            "DOMAINS",
            "SCHEMATA",
            "COLUMN_PRIVILEGES",
            "CONSTRAINT_TABLE_USAGE",
            "COLUMNS",
            "ROUTINES",
            "CONSTRAINT_COLUMN_USAGE",
            "TABLE_PRIVILEGES",
            "TABLES",
            "CHECK_CONSTRAINTS",
            "VIEW_COLUMN_USAGE"

    ));

    private SQLServerJdbcMetaDataExtractor(){}

    public static JdbcMetaDataExtractor newInstance(){
        return new SQLServerJdbcMetaDataExtractor();
    }

    @Override
    protected boolean isSysCatalog(String catalogName) {
        return catalogName!=null && SYS_CAT.contains(catalogName.toLowerCase());
    }

    @Override
    protected boolean isSysSchema(String schemaName) {
        return schemaName!=null && SYS_SCHEMA.contains(schemaName.toLowerCase());
    }

    @Override
    protected boolean isSysTable(String tableName) {
        return tableName!=null && SYS_TABLE.contains(tableName.toUpperCase());
    }

    @Override
    protected ResultSet decideDatabase(DatabaseMetaData metaData) throws SQLException {
        return metaData.getCatalogs();
    }

    @Override
    public List<Map<String, Object>> explain(Connection connection, String sql) throws SQLException {
        connection.setReadOnly(true);
        List<Map<String, Object>> result = Collections.emptyList();
        Statement stmt = connection.createStatement();
        stmt.execute("SET SHOWPLAN_TEXT ON");
        //stmt.execute("GO");
        stmt.execute("SET NOEXEC ON");
        //stmt.execute("GO");
        try(ResultSet resultSet = stmt.executeQuery(sql)){
            result = extractResultSetMap(resultSet);
        }
        //stmt.execute("GO");
        stmt.execute("SET NOEXEC OFF");
        //stmt.execute("GO");
        stmt.execute("SET SHOWPLAN_TEXT OFF");
        //stmt.execute("GO");
        stmt.closeOnCompletion();
        return result;
    }

    /**
     *  TABLE_CAT
     *  TABLE_SCHEM
     *  TABLE_NAME
     *  COLUMN_NAME
     *  DATA_TYPE
     *  TYPE_NAME
     *  COLUMN_SIZE
     *  BUFFER_LENGTH
     *  DECIMAL_DIGITS
     *  NUM_PREC_RADIX
     *  NULLABLE
     *  REMARKS
     *  COLUMN_DEF
     *  SQL_DATA_TYPE
     *  SQL_DATETIME_SUB
     *  CHAR_OCTET_LENGTH
     *  ORDINAL_POSITION
     *  IS_NULLABLE
     *  SS_IS_SPARSE
     *  SS_IS_COLUMN_SET
     *  IS_GENERATEDCOLUMN
     *  IS_AUTOINCREMENT
     *  SS_UDT_CATALOG_NAME
     *  SS_UDT_SCHEMA_NAME
     *  SS_UDT_ASSEMBLY_TYPE_NAME
     *  SS_XML_SCHEMACOLLECTION_CATALOG_NAME
     *  SS_XML_SCHEMACOLLECTION_SCHEMA_NAME
     *  SS_XML_SCHEMACOLLECTION_NAME
     *  SS_DATA_TYPE
     * @param resultSet
     * @return
     * @throws SQLException
     */
    @Override
    protected List<RawColumn> parseToColumn(ResultSet resultSet) throws SQLException {
        List<RawColumn> rawColumns = new LinkedList<>();
        while (resultSet.next()){
            RawColumn column = new RawColumn();
            column.setTABLE_CAT(resultSet.getString(1 /*"TABLE_CAT"*/));
            if (isSysCatalog(column.getTABLE_CAT())){
                continue;
            }
            column.setTABLE_SCHEM(resultSet.getString(2 /*"TABLE_SCHEM"*/));
            if (isSysSchema(column.getTABLE_SCHEM())){
                continue;
            }
            column.setTABLE_NAME(resultSet.getString(3 /*"TABLE_NAME"*/));
            if (isSysTable(column.getTABLE_NAME())){
                continue;
            }
            column.setCOLUMN_NAME(resultSet.getString(4 /*"COLUMN_NAME"*/));
            column.setDATA_TYPE(resultSet.getInt(5 /*"DATA_TYPE"*/));
            column.setTYPE_NAME(resultSet.getString(6 /*"TYPE_NAME"*/));
            column.setCOLUMN_SIZE(resultSet.getInt(7 /*"COLUMN_SIZE"*/));
            //column.setBUFFER_LENGTH(resultSet.getInt(8 /*"BUFFER_LENGTH"*/));
            if (isFloatingNumber(column.getTYPE_NAME())){
                column.setDECIMAL_DIGITS(resultSet.getInt(9 /*"DECIMAL_DIGITS"*/));
            }
            column.setNUM_PREC_RADIX(resultSet.getInt(10 /*"NUM_PREC_RADIX"*/));
            column.setNULLABLE(resultSet.getInt(11 /*"NULLABLE"*/));
            column.setREMARKS(resultSet.getString(12 /*"REMARKS"*/));
            column.setCOLUMN_DEF(resultSet.getString(13 /*"COLUMN_DEF"*/));
            //column.setSQL_DATA_TYPE(resultSet.getInt(14 /*"SQL_DATA_TYPE"*/));
            //column.setSQL_DATETIME_SUB(resultSet.getInt(15 /*"SQL_DATETIME_SUB"*/));
            column.setCHAR_OCTET_LENGTH(resultSet.getInt(16 /*"CHAR_OCTET_LENGTH"*/));
            column.setORDINAL_POSITION(resultSet.getInt(17 /*"ORDINAL_POSITION"*/));
            column.setIS_NULLABLE(resultSet.getString(18 /*"IS_NULLABLE"*/));
            column.setIS_AUTOINCREMENT(resultSet.getString(22 /*IS_AUTO_INCREMENT*/));
            column.setIS_GENERATEDCOLUMN(resultSet.getString(21 /*"IS_GENERATEDCOLUMN"*/));
            rawColumns.add(column);
        }
        DatasourceResolver.close(resultSet);
        return rawColumns;
    }

    enum DATATYPE{
      BIT( "bit","BIT"),
      TINYINT( "tinyint","TINYINT"),
      SMALLINT( "smallint","SMALLINT"),
      INT( "int","INT"),
      BIGINT( "bigint","BIGINT"),
      DECIMAL( "decimal","DECIMAL($p,$s)"),
      REAL( "real","REAL"),
      FLOAT( "float","FLOAT($l)"),
      NUMERIC( "numeric","NUMERIC($p,$s)"),
      SMALLMONEY( "smallmoney","SMALLMONEY"),
      MONEY( "money","MONEY"),
      TIMESTAMP( "timestamp","TIMESTAMP"),
      DATETIME( "datetime","DATETIME"),
      SMALLDATETIME( "smalldatetime","SMALLDATETIME"),
      CHAR( "char","CHAR($l)"),
      NCHAR( "nchar","NCHAR($l)"),
      VARCHAR( "varchar","VARCHAR($l)"),
      NVARCHAR( "nvarchar","NVARCHAR($l)"),
      TEXT( "text","TEXT"),
      NTEXT( "ntext","NTEXT"),
      BINARY( "binary","BINARY($l)"),
      VARBINARY( "varbinary","VARBINARY($l)"),
      IMAGE( "image","IMAGE"),
      UNIQUEIDENTIFIER( "uniqueidentifier","UNIQUEIDENTIFIER"),
      CURSOR( "cursor","CURSOR"),    //SQLServer2000游标类型
      XML( "xml","XML");

      private String name;
      private String pattern;

      private DATATYPE(String name, String pattern) {
        this.name = name;
        this.pattern = pattern;
      }

      public static String getTypeName(String name, int length, int precision, int scale){
        DATATYPE type = valueOf(name.toUpperCase());
        switch (type){
          case DECIMAL:
          case NUMERIC:
            if (scale>0){
              if (precision<=0) precision = 19;
              String n = type.pattern.replace("$s", String.valueOf(scale));
              return n.replace("$p",String.valueOf(precision));
            }
            if (precision>0){
              return type.pattern.replace("$p,$s", String.valueOf(precision));
            }
            return type.pattern.replace("\\(.*\\)","");
          case FLOAT:
          case CHAR:
          case NCHAR:
          case VARCHAR:
          case NVARCHAR:
          case BINARY:
          case VARBINARY:
            if (length>0){
              return type.pattern.replace("$l", String.valueOf(length));
            } else {
              return type.pattern.replace("\\($l\\)", "");
            }
          default:
            return type.pattern;
        }
      }
    }

    //@Override
    public String extractDDDL(Connection connection, String ddlType, String tableCatalog, String tableSchema, String tableName) throws SQLException {
      String columns = String.format("SELECT * FROM INFORMATION_SCHEMA.TABLES AS T INNER JOIN     INFORMATION_SCHEMA.COLUMNS AS C ON \n                                    T.TABLE_NAME = C.TABLE_NAME                                    AND T.TABLE_SCHEMA = C.TABLE_SCHEMA                                    AND T.TABLE_CATALOG = C.TABLE_CATALOG  WHERE      T.TABLE_NAME = N'%s'  AND T.TABLE_SCHEMA = N'%s'  AND T.TABLE_CATALOG = N'%s'", tableName, tableSchema, tableCatalog);
      String pkColumnsQuery = String.format("SELECT U.COLUMN_NAME FROM  INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS C  INNER JOIN  INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS U ON U.CONSTRAINT_NAME = C.CONSTRAINT_NAME  WHERE C.CONSTRAINT_TYPE = 'PRIMARY KEY'  AND C.TABLE_NAME = N'%s'  AND C.TABLE_SCHEMA = N'%s'  AND C.TABLE_CATALOG = N'%s'", tableName, tableSchema, tableCatalog);
      Statement statements = connection.createStatement();
      List<String> pkColumns = extractResultSetArray(statements.executeQuery(pkColumnsQuery)).stream().map((Object[] o)->o[0]).map(i->(String)i).collect(Collectors.toList());
      boolean isSinglePk = pkColumns.size() == 1;
      int spaces = 2;
      StringBuilder ddlBuilder = new StringBuilder();
      ddlBuilder.append("CREATE TABLE ").append(tableName).append(" (");
      List<Map<String, Object>> columnMaps = extractResultSetMap(statements.executeQuery(columns));
      for (Map<String, Object> map: columnMaps){
        String name = stringfy(map.get("COLUMN_NAME"));
        String defaults = stringfy(map.getOrDefault("COLUMN_DEFAULT",""));
        String type = stringfy(map.get("DATA_TYPE"));
        String length = stringfy(map.getOrDefault("CHARACTER_MAXIMUM_LENGTH",""));
        String precision = stringfy(map.getOrDefault("NUMERIC_PRECISION",""));
        String scale = stringfy(map.getOrDefault("NUMERIC_SCALE",""));
        String isNullableStr = stringfy(map.getOrDefault("IS_NULLABLE",""));
        boolean isNotNull = "NO".equals(isNullableStr);
        length = isMax(type, length) ? String.valueOf(2147483647) : length;
        //String substitution = this.getTypeWithSubstitution(type != null ? type : "<type>", StringUtil.parseInt(length, -1), StringUtil.parseInt(precision, -1), StringUtil.parseInt(scale, -1));
        //ddl.newLine().space(spaces).columnRef(name != null ? name : "<name>").space().type(substitution);
        ddlBuilder.append("\n").append("  ").append(name).append(" ").append(DATATYPE.getTypeName(type, parseInt(length, -1), parseInt(precision,-1), parseInt(scale, -1)));
        if (isSinglePk && ((String)pkColumns.iterator().next()).equals(name)) {
          ddlBuilder.append(" ").append("PRIMARY KEY");
        }

        if (defaults != null) {
          ddlBuilder.append(" ").append("DEFAULT").append(" ").append(defaults);
        }

        if (isNotNull) {
          ddlBuilder.append(" ").append("NOT NULL");
        }
        ddlBuilder.append(",");
      }
      if (columnMaps.size()>0){
        ddlBuilder.delete(ddlBuilder.length()-1, ddlBuilder.length());
      }
      ddlBuilder.append("\n)");

      String pk;
      if (pkColumns.size() > 1) {
        ddlBuilder.append(",").append("\n").append(" ").append("PRIMARY KEY").append(" ").append("(");
        boolean f = true;
        for(Iterator var14 = pkColumns.iterator(); var14.hasNext(); /*ddl.identifier(pk)*/) {
          pk = (String)var14.next();
          if (f) {
            f = false;
          } else {
            ddlBuilder.append(",").append(" ");
          }
        }
        ddlBuilder.append(")");
      }

      System.out.println(ddlBuilder.toString());

      String fkQuery = String.format("SELECT C.CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS C WHERE      C.CONSTRAINT_TYPE = 'FOREIGN KEY'  AND C.TABLE_NAME = N'%s'  AND C.TABLE_SCHEMA = N'%s'  AND C.TABLE_CATALOG = N'%s'", tableName, tableSchema, tableCatalog);
      /*List<String> fkNames = (List)statements.simple().noisy().execute(fkQuery, StandardExecutionMode.QUERY, StandardResultsProcessors.resultsTransformer((rsx) -> {
        List result = ContainerUtil.newSmartList();

        while(rsx.next()) {
          result.add(rsx.getString("constraint_name"));
        }

        return result;
      }, ContainerUtil.emptyList()));
      Iterator var51 = fkNames.iterator();

      while(var51.hasNext()) {
        String fkName = (String)var51.next();
        String fkCols = String.format("SELECT\n    U.COLUMN_NAME AS FROM_COL,    K.TABLE_NAME,    K.COLUMN_NAME  FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS C   INNER JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS U ON U.CONSTRAINT_NAME = C.CONSTRAINT_NAME  INNER JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS R ON R.CONSTRAINT_NAME = C.CONSTRAINT_NAME  INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE K ON K.CONSTRAINT_NAME = R.UNIQUE_CONSTRAINT_NAME  WHERE C.CONSTRAINT_NAME = '%s'", fkName);
        Set<String> fromCols = ContainerUtil.newLinkedHashSet();
        Set<String> toCols = ContainerUtil.newLinkedHashSet();
        String refTableName = "<ref_table>";
        ClosableResultsProducer producer = statements.simple().noisy().execute(fkCols, StandardExecutionMode.QUERY);
        Throwable var22 = null;

        try {
          ResultSet rs = producer.advance() ? (ResultSet)producer.processCurrent(StandardResultsProcessors.RESULT_SET) : null;
          if (rs != null) {
            while(rs.next()) {
              refTableName = rs.getString("table_name");
              ContainerUtil.addIfNotNull(fromCols, rs.getString("from_col"));
              ContainerUtil.addIfNotNull(toCols, rs.getString("column_name"));
            }
          }
        } catch (Throwable var47) {
          var22 = var47;
          throw var47;
        } finally {
          if (producer != null) {
            if (var22 != null) {
              try {
                producer.close();
              } catch (Throwable var44) {
                var22.addSuppressed(var44);
              }
            } else {
              producer.close();
            }
          }

        }

        ddl.symbol(",").newLine().space(spaces).keywords(new String[]{"foreign", "key"}).space().symbol("(");
        DialectUtils.appendStringList(ddl, fromCols);
        ddl.symbol(")");
        ddl.space().keyword("references").space().identifier(refTableName).space().symbol("(");
        DialectUtils.appendStringList(ddl, toCols);
        ddl.symbol(")");
      }

      ddl.newLine().symbol(")").symbol(";");
      pk = String.format("select  ind.name as index_name,  t.name as table_name,  col.name as col_name,  ind.is_unique from sys.indexes ind  inner join sys.index_columns ic on ind.object_id = ic.object_id and ind.index_id = ic.index_id  inner join sys.columns col on ic.object_id = col.object_id and ic.column_id = col.column_id  inner join sys.tables t on ind.object_id = t.object_id where ind.is_primary_key = 0 and t.is_ms_shipped = 0  and t.name = N'%s'", tableName);
      MultiMap<String, String> indexColumns = new MultiMap();
      Map<String, Boolean> indexUnique = new HashMap();
      ClosableResultsProducer producer = statements.simple().noisy().execute(pk, StandardExecutionMode.QUERY);
      Throwable var56 = null;

      try {
        ResultSet rs = producer.advance() ? (ResultSet)producer.processCurrent(StandardResultsProcessors.RESULT_SET) : null;
        if (rs != null) {
          while(rs.next()) {
            String indexName = rs.getString("index_name");
            String columnName = rs.getString("col_name");
            boolean isUnique = rs.getBoolean("is_unique");
            indexColumns.putValue(indexName, columnName);
            indexUnique.put(indexName, isUnique);
          }
        }
      } catch (Throwable var45) {
        var56 = var45;
        throw var45;
      } finally {
        if (producer != null) {
          if (var56 != null) {
            try {
              producer.close();
            } catch (Throwable var43) {
              var56.addSuppressed(var43);
            }
          } else {
            producer.close();
          }
        }

      }

      Iterator var55 = indexColumns.keySet().iterator();

      while(var55.hasNext()) {
        String i = (String)var55.next();
        Collection<String> colNames = indexColumns.get(i);
        Boolean isUnique = (Boolean)indexUnique.get(i);
        ddl.newLine().keywords(new String[]{"create"}).space();
        if (isUnique) {
          ddl.keyword("unique").space();
        }

        ddl.keyword("index").space().identifier(i).space().keyword("on").space().identifier(tableName).space().symbol("(");
        boolean first = true;

        String col;
        for(Iterator var65 = colNames.iterator(); var65.hasNext(); ddl.identifier(col)) {
          col = (String)var65.next();
          if (first) {
            first = false;
          } else {
            ddl.symbol(",").space();
          }
        }

        ddl.symbol(")").symbol(";");
      }

      String var10000 = ddl.getStatement();
      if (var10000 == null) {
        $$$reportNull$$$0(20);
      }*/

      return "";
    }

  private int parseInt(String s, int defaultValue){
      try {
        return Integer.valueOf(s);
      }catch (NumberFormatException e){
        return -1;
      }
  }

  private String stringfy(Object o){
      if (o == null) return null;
      return o.toString();
  }

  private static boolean isMax(@Nullable String type, @Nullable String length) {
    return type != null && length != null && Integer.valueOf(length)<0 && !isLargeType(type);
  }

  private static boolean isLargeType(@Nullable String type) {
    return Arrays.asList("xml", "image", "text", "ntext").stream().anyMatch(i->i.equalsIgnoreCase(type));
  }

  public static void main(String[] args) throws SQLException, ClassNotFoundException {
    SQLServerJdbcMetaDataExtractor extractor = (SQLServerJdbcMetaDataExtractor) SQLServerJdbcMetaDataExtractor.newInstance();
    Connection connection = DatasourceResolver.getConnection("jdbc:sqlserver://192.168.2.60;DatabaseName=dmp", "sa", "liangzhi123");
    extractor.extractDDL(connection, "TABLE","dmp","dbo","md_column");
  }
}
