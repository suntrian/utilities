package dev.suntr.extract.parser;

import com.sun.istack.internal.Nullable;
import dev.suntr.extract.datasource.DatasourceResolver;
import dev.suntr.extract.entity.*;

import java.sql.*;
import java.util.*;

@SuppressWarnings("Duplicates")
public abstract class DefaultJdbcMetaDataExtractor implements JdbcMetaDataExtractor {

  public static class DefaultJdbcMetaDataExtractorImpl extends DefaultJdbcMetaDataExtractor {
        @Override
        protected boolean isSysCatalog(String catalogName) {
            return false;
        }

        @Override
        protected boolean isSysSchema(String schemaName) {
            return false;
        }

        @Override
        protected boolean isSysTable(String tableName) {
            return false;
        }

        @Override
        public ResultSet decideDatabase(DatabaseMetaData metaData) throws SQLException {
            return metaData.getCatalogs();
        }
    }


    protected DatabaseMetaData metaData = null;

    protected static JdbcMetaDataExtractor instance;

    private static final Integer execQueryTimeout = 10*60;

    protected DefaultJdbcMetaDataExtractor(){}

    public static JdbcMetaDataExtractor newInstance(){
        return instance = new DefaultJdbcMetaDataExtractorImpl();
    }

    protected abstract boolean isSysCatalog(String catalogName);

    protected  abstract boolean isSysSchema(String schemaName);

    protected abstract boolean isSysTable(String tableName);

    /**
     * 是否浮点类型，浮点类型保留精度，否则不保留精度
     * @param dataType
     * @return
     */
    public static boolean isFloatingNumber(String dataType) {
        dataType = dataType.toLowerCase();
        return dataType.startsWith("num")       // for number and numeric
                || dataType.startsWith("dec")   //for decimal and dec
                || dataType.contains("double")  //for double and BINARY_DOUBLE  and DOUBLE UNSIGNED...
                || dataType.contains("float")   //for float and BINARY_FLOAT and FLOAT UNSIGNED...
                || dataType.equals("real");     //for real
    }

    protected abstract ResultSet decideDatabase(DatabaseMetaData metaData) throws SQLException ;

    protected List<RawDatabase> parseToDatabase(ResultSet resultSet) throws SQLException {
        final int columnSize = resultSet.getMetaData().getColumnCount();
        List<RawDatabase> rawDatabases = new LinkedList<>();
        while (resultSet.next()){
            RawDatabase database = new RawDatabase();
            if (columnSize == 1){
                database.setTABLE_CAT(resultSet.getString(1 /*"TABLE_CAT"*/));
            } else {
                database.setTABLE_SCHEM(resultSet.getString(1 /*TABLE_SCHEM*/));
                database.setTABLE_CAT(resultSet.getString(2 /*"TABLE_CATALOG"*/));
            }
            if(isSysCatalog(database.getTABLE_CAT())){
                continue;
            }
            if (isSysSchema(database.getTABLE_SCHEM())){
                continue;
            }
            rawDatabases.add(database);
        }
        DatasourceResolver.close(resultSet);
        return rawDatabases;
    }

    protected List<RawTable> parseToTable(ResultSet resultSet) throws SQLException {
        final int columnSize = resultSet.getMetaData().getColumnCount();
        List<RawTable> rawTables = new LinkedList<>();
        while (resultSet.next()){
            RawTable table = new RawTable();
            table.setTABLE_CAT(resultSet.getString(1 /*"TABLE_CAT"*/));
            if (isSysCatalog(table.getTABLE_CAT())){
                continue;
            }
            table.setTABLE_SCHEM(resultSet.getString(2 /*"TABLE_SCHEM"*/));
            if (isSysSchema(table.getTABLE_SCHEM())){
                continue;
            }
            table.setTABLE_NAME(resultSet.getString(3 /*"TABLE_NAME"*/));
            if (isSysTable(table.getTABLE_NAME())){
                continue;
            }
            rawTables.add(table);
            table.setTABLE_TYPE(resultSet.getString(4 /*"TABLE_TYPE"*/));
            table.setREMARKS(resultSet.getString(5 /*"REMARK"*/));
            if (columnSize <6 ) { /*for sqlserver and oracle and postgresql */ continue; }
            table.setTYPE_CAT(resultSet.getString(6 /*"TYPE_CAT"*/));
            table.setTYPE_SCHEM(resultSet.getString(7 /*"TYPE_SCHEM"*/));
            table.setTYPE_NAME(resultSet.getString(8 /*"TYPE_NAME"*/));
            table.setSELF_REFERENCING_COL_NAME(resultSet.getString(9 /*"SELF_REFERENCING_COL_NAME"*/));
            table.setREF_GENERATION(resultSet.getString(10 /*"REF_GENERATION"*/));
        }
        DatasourceResolver.close(resultSet);
        return rawTables;
    }

    protected List<RawColumn> parseToColumn(ResultSet resultSet) throws SQLException {
        final int columnSize = resultSet.getMetaData().getColumnCount();
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
            rawColumns.add(column);
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
            if (columnSize < 19) {/* for oracle */ continue;}
            // sql server has different realization of fellow
            column.setSCOPE_CATALOG(resultSet.getString(19 /*"SCOPE_CATALOG"*/));
            column.setSCOPE_SCHEMA(resultSet.getString(20 /*"SCOPE_SCHEMA"*/));
            column.setSCOPE_TABLE(resultSet.getString(21 /*"SCOPE_TABLE"*/));
            column.setSOURCE_DATA_TYPE(resultSet.getShort(22 /*"SOURCE_DATA_TYPE"*/));
            column.setIS_AUTOINCREMENT(resultSet.getString(23 /*"IS_AUTOINCREMENT"*/));
            if (columnSize < 24) { /* for hive and postgresql */ continue;}
            column.setIS_GENERATEDCOLUMN(resultSet.getString(24 /*"IS_GENERATEDCOLUMN"*/));
        }
        DatasourceResolver.close(resultSet);
        return rawColumns;
    }

    protected List<RawPrimaryKey> parseToPrimaryKey(ResultSet resultSet) throws SQLException {
        List<RawPrimaryKey> rawPrimaryKeys = new LinkedList<>();
        while (resultSet.next()){
            RawPrimaryKey primaryKey = new RawPrimaryKey();
            primaryKey.setTABLE_CAT(resultSet.getString(1 /*"TABLE_CAT"*/));
            if (isSysCatalog(primaryKey.getTABLE_CAT())){
              continue;
            }
            primaryKey.setTABLE_SCHEM(resultSet.getString(2 /*"TABLE_SCHEM"*/));
            if (isSysSchema(primaryKey.getTABLE_SCHEM())){
              continue;
            }
            primaryKey.setTABLE_NAME(resultSet.getString(3 /*"TABLE_NAME"*/));
            primaryKey.setCOLUMN_NAME(resultSet.getString(4 /*"COLUMN_NAME"*/));
            primaryKey.setKEY_SEQ(resultSet.getShort(5 /*"KEY_SEQ"*/));
            primaryKey.setPK_NAME(resultSet.getString(6 /*"PK_NAME"*/));
            rawPrimaryKeys.add(primaryKey);
        }
        DatasourceResolver.close(resultSet);
        return rawPrimaryKeys;
    }

    protected List<RawIndexInfo> parseToIndexInfo(ResultSet resultSet) throws SQLException {
        List<RawIndexInfo> rawIndexInfos = new LinkedList<>();
        while (resultSet.next()){
            RawIndexInfo indexInfo = new RawIndexInfo();
            indexInfo.setTABLE_CAT(resultSet.getString(1 /*"TABLE_CAT"*/));
            if (isSysCatalog(indexInfo.getTABLE_CAT())){
              continue;
            }
            indexInfo.setTABLE_SCHEM(resultSet.getString(2 /*"TABLE_SCHEM"*/));
            if (isSysSchema(indexInfo.getTABLE_SCHEM())){
              continue;
            }
            indexInfo.setTABLE_NAME(resultSet.getString(3 /*"TABLE_NAME"*/));
            indexInfo.setNON_UNIQUE(resultSet.getBoolean(4 /*"NON_UNIQUE"*/));
            indexInfo.setINDEX_QUALIFIER(resultSet.getString(5 /*"INDEX_QUALIFIER"*/));
            indexInfo.setINDEX_NAME(resultSet.getString(6 /*"INDEX_NAME"*/));
            indexInfo.setTYPE(resultSet.getShort(7 /*"TYPE"*/));
            indexInfo.setORDINAL_POSITION(resultSet.getShort(8 /*"ORDINAL_POSITION"*/));
            indexInfo.setCOLUMN_NAME(resultSet.getString(9 /*"COLUMN_NAME"*/));
            indexInfo.setASC_OR_DESC(resultSet.getString(10 /*"ASC_OR_DESC"*/));
            indexInfo.setCARDINALITY(resultSet.getLong(11 /*"CARDINALITY"*/));
            indexInfo.setPAGES(resultSet.getLong(12 /*"PAGES"*/));
            indexInfo.setFILTER_CONDITION(resultSet.getString(13 /*"FILTER_CONDITION"*/));
            rawIndexInfos.add(indexInfo);
        }
        DatasourceResolver.close(resultSet);
        return rawIndexInfos;
    }

    protected List<RawForeignKeyInfo> parseToForeignKeyInfo(ResultSet resultSet) throws SQLException {
        List<RawForeignKeyInfo> rawForeignKeyInfos = new LinkedList<>();
        while (resultSet.next()){
            RawForeignKeyInfo foreignKeyInfo = new RawForeignKeyInfo();
            foreignKeyInfo.setPKTABLE_CAT(resultSet.getString(1 /*"PKTABLE_CAT"*/));
            if (isSysCatalog(foreignKeyInfo.getPKTABLE_CAT())){
              continue;
            }
            foreignKeyInfo.setPKTABLE_SCHEM(resultSet.getString(2 /*"PKTABLE_SCHEM"*/));
            if (isSysSchema(foreignKeyInfo.getPKTABLE_SCHEM())){
              continue;
            }
            foreignKeyInfo.setPKTABLE_NAME(resultSet.getString(3 /*"PKTABLE_NAME"*/));
            foreignKeyInfo.setPKCOLUMN_NAME(resultSet.getString(4 /*"PKCOLUMN_NAME"*/));
            foreignKeyInfo.setFKTABLE_CAT(resultSet.getString(5 /*"FKTABLE_CAT"*/));
            if (isSysCatalog(foreignKeyInfo.getFKTABLE_CAT())){
              continue;
            }
            foreignKeyInfo.setFKTABLE_SCHEM(resultSet.getString(6 /*"FKTABLE_SCHEM"*/));
            if (isSysSchema(foreignKeyInfo.getFKTABLE_SCHEM())){
              continue;
            }
            foreignKeyInfo.setFKTABLE_NAME(resultSet.getString(7 /*"FKTABLE_NAME"*/));
            foreignKeyInfo.setFKCOLUMN_NAME(resultSet.getString(8 /*"FKCOLUMN_NAME"*/));
            foreignKeyInfo.setKEY_SEQ(resultSet.getShort(9 /*"KEY_SEQ"*/));
            foreignKeyInfo.setUPDATE_RULE(resultSet.getShort(10 /*"UPDATE_RULE"*/));
            foreignKeyInfo.setDELETE_RULE(resultSet.getShort(11 /*"DELETE_RULE"*/));
            foreignKeyInfo.setFK_NAME(resultSet.getString(12 /*"FK_NAME"*/));
            foreignKeyInfo.setPK_NAME(resultSet.getString(13 /*"PK_NAME"*/));
            rawForeignKeyInfos.add(foreignKeyInfo);
        }
        return rawForeignKeyInfos;
    }

    protected List<RawRoutine> parseToRawRoutine(ResultSet resultSet) throws SQLException {
      List<RawRoutine> routines = new LinkedList<>();
      while (resultSet.next()){
        RawRoutine routine = new RawRoutine();
        routine.setROUTINE_CAT(resultSet.getString(1));
        if (isSysCatalog(routine.getROUTINE_CAT())){
          continue;
        }
        routine.setROUTINE_SCHEMA(resultSet.getString(2));
        if (isSysSchema(routine.getROUTINE_SCHEMA())){
          continue;
        }
        routine.setROUTINE_NAME(resultSet.getString(3));
        routine.setROUTINE_TYPE(resultSet.getString(4));
        routine.setROUTINE_DEFINITION(resultSet.getString(5));
        routine.setCREATED(resultSet.getDate(8));
        routine.setUPDATED(resultSet.getDate(9));
        routines.add(routine);
      }
      return routines;
    }

    @Override
    public List<RawDatabase> extractDatabase(Connection connection) throws SQLException {
        getMetaData(connection);
        return parseToDatabase(decideDatabase(metaData));
    }

    @Override
    public List<RawTable> extractTable(Connection connection, @Nullable String catalog, @Nullable String schemaPattern, @Nullable String tableNamePattern, @Nullable String... types) throws SQLException {
        getMetaData(connection);
        ResultSet resultSet = metaData.getTables(catalog, schemaPattern, tableNamePattern, types);
        return parseToTable(resultSet);
    }

    @Override
    public List<RawColumn> extractColumn(Connection connection, @Nullable String catalog, @Nullable String schemaPattern, @Nullable String tableNamePattern, @Nullable String columnNamePattern) throws SQLException {
        getMetaData(connection);
        ResultSet resultSet = metaData.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
        return parseToColumn(resultSet);
    }

    @Override
    public List<RawPrimaryKey> extractPrimaryKey(Connection connection, @Nullable String catalog, @Nullable String schema, String tableName) throws SQLException {
        getMetaData(connection);
        ResultSet resultSet = metaData.getPrimaryKeys(catalog, schema, tableName);
        return parseToPrimaryKey(resultSet);
    }

    @Override
    public List<RawIndexInfo> extractIndexInfo(Connection connection, @Nullable String catalog, @Nullable String schema, String table, boolean unique, boolean approximate) throws SQLException {
        getMetaData(connection);
        ResultSet resultSet = metaData.getIndexInfo(catalog, schema, table, unique, approximate);
        return parseToIndexInfo(resultSet);
    }

    @Override
    public List<RawForeignKeyInfo> extractExportedKey(Connection connection, @Nullable String catalog, @Nullable String schema, String table) throws SQLException {
        getMetaData(connection);
        ResultSet resultSet = metaData.getExportedKeys(catalog, schema, table);
        return parseToForeignKeyInfo(resultSet);
    }

    @Override
    public List<RawForeignKeyInfo> extractImportedKey(Connection connection, @Nullable String catalog, @Nullable String schema, String table) throws SQLException {
        getMetaData(connection);
        ResultSet resultSet = metaData.getImportedKeys(catalog, schema, table);
        return parseToForeignKeyInfo(resultSet);
    }

    @Override
    public  List<Object[]> extractArray(Connection connection, String sql) throws SQLException {
        Statement stmt = DatasourceResolver.statement(connection);
        stmt.setQueryTimeout(execQueryTimeout);
        ResultSet resultSet = stmt.executeQuery(sql);
        List<Object[]> list = extractResultSetArray(resultSet);
        resultSet.close();
        stmt.close();
        return list;
    }

    @Override
    public List<Object[]> extractArray(Connection connection, String sql, Object... args) throws SQLException {
        PreparedStatement stmt = DatasourceResolver.statement(connection, sql, args);
        stmt.setQueryTimeout(execQueryTimeout);
        ResultSet resultSet = stmt.executeQuery();
        List<Object[]> results = extractResultSetArray(resultSet);
        resultSet.close();
        stmt.close();
        return results;
    }

    @Override
    public List<Map<String, Object>> extractMap(Connection connection, String sql) throws SQLException {
        PreparedStatement stmt = DatasourceResolver.statement(connection, sql);
        ResultSet resultSet = stmt.executeQuery();
        List<Map<String, Object>> result = extractResultSetMap(resultSet);
        resultSet.close();
        stmt.close();
        return result;
    }

    @Override
    public List<Map<String, Object>> extractMap(Connection connection, String sql, Object... args) throws SQLException {
        PreparedStatement stmt = DatasourceResolver.statement(connection, sql, args);
        stmt.setQueryTimeout(execQueryTimeout);
        ResultSet resultSet = stmt.executeQuery();
        List<Map<String, Object>> list = extractResultSetMap(resultSet);
        resultSet.close();
        stmt.close();
        return list;
    }

    @Override
    public List<Map<String, Object>> explain(Connection connection, String sql) throws SQLException {
        String explainSql = "EXPLAIN " + sql;
        return extractMap(connection, explainSql);
    }

  @Override
  public boolean supportRoutine() {
    return false;
  }

  @Override
  public boolean supportDdl() {
    return false;
  }

  @Override
    public String getVersion(Connection connection) throws SQLException {
        return getMetaData(connection).getDatabaseProductVersion();
    }

    @Override
    public String extractDDL(Connection connection, String ddlType, String catalog, String schema, String name) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<RawRoutine> extractRoutine(Connection connection, String type, String catalog, String schema) throws SQLException {
        throw new UnsupportedOperationException();
    }


  private DatabaseMetaData getMetaData(Connection connection) throws SQLException {
        if (this.metaData == null){
            this.metaData = connection.getMetaData();
        }
        return this.metaData;
    }

    public void close(){
        this.metaData = null;
        DefaultJdbcMetaDataExtractor.instance = null;
    }

    protected List<Object[]> extractResultSetArray(ResultSet resultSet) throws SQLException {
        final int columnSize = resultSet.getMetaData().getColumnCount();
        List<Object[]> objects = new LinkedList<>();
        while (resultSet.next()){
            Object[] obj = new Object[columnSize];
            for (int i = 0; i < columnSize; i++){
                obj[i] = resultSet.getObject(i+1);
            }
            objects.add(obj);
        }
        DatasourceResolver.close(resultSet);
        return objects;
    }

    protected List<Map<String, Object>> extractResultSetMap(ResultSet resultSet) throws SQLException {
        final int columnSize = resultSet.getMetaData().getColumnCount();
        List<String> columnLabels = new ArrayList<>(columnSize);
        //List<String> columnNames = new ArrayList<>(columnSize);
        for (int i = 0; i < columnSize; i++){
            columnLabels.add(resultSet.getMetaData().getColumnLabel(i+1));
            //columnNames.add(resultSet.getMetaData().getColumnName(i+1));
        }
        List<Map<String, Object>> maps = new LinkedList<>();
        while (resultSet.next()){
            Map<String, Object> map = new HashMap<>(columnSize*3);
            for (int i = 0; i < columnSize; i++){
                map.put(columnLabels.get(i), resultSet.getObject(i+1));
                //map.put(columnNames.get(i), resultSet.getObject(i+1));
            }
            maps.add(map);
        }
        DatasourceResolver.close(resultSet);
        return maps;
    }
}
