package dev.suntr.extract.parser;

import com.sun.istack.internal.Nullable;
import dev.suntr.extract.entity.*;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface JdbcMetaDataExtractor extends Closeable {


  List<RawDatabase> extractDatabase(Connection connection) throws SQLException ;


  /**
   * TABLE_CAT
   * TABLE_SCHEM
   * TABLE_NAME
   * TABLE_TYPE
   * REMARKS
   * TYPE_CAT
   * TYPE_SCHEM
   * TYPE_NAME
   * SELF_REFERENCING_COL_NAME
   * REF_GENERATION
   *
   * @param connection 数据库连接
   * @param catalog a catalog name; must match the catalog name as it
   *        is stored in the database; "" retrieves those without a catalog;
   *        <code>null</code> means that the catalog name should not be used to narrow
   *        the search
   * @param schemaPattern a schema name pattern; must match the schema name
   *        as it is stored in the database; "" retrieves those without a schema;
   *        <code>null</code> means that the schema name should not be used to narrow
   *        the search
   * @param tableNamePattern a table name pattern; must match the
   *        table name as it is stored in the database
   * @param types a list of table types, which must be from the list of table types
   *         returned from {@link DatabaseMetaData#getTableTypes},to include; <code>null</code> returns
   * all types
   * @return
   */
  List<RawTable> extractTable(Connection connection,
                              @Nullable String catalog,
                              @Nullable String schemaPattern,
                              @Nullable String tableNamePattern,
                              @Nullable String... types) throws SQLException;

  /**
   *
   * TABLE_CAT
   * TABLE_SCHEM
   * TABLE_NAME
   * COLUMN_NAME
   * DATA_TYPE
   * TYPE_NAME
   * COLUMN_SIZE
   * BUFFER_LENGTH
   * DECIMAL_DIGITS
   * NUM_PREC_RADIX
   * NULLABLE
   * REMARKS
   * COLUMN_DEF
   * SQL_DATA_TYPE
   * SQL_DATETIME_SUB
   * CHAR_OCTET_LENGTH
   * ORDINAL_POSITION
   * IS_NULLABLE
   * SCOPE_CATALOG
   * SCOPE_SCHEMA
   * SCOPE_TABLE
   * SOURCE_DATA_TYPE
   * IS_AUTOINCREMENT
   * IS_GENERATEDCOLUMN
   * @param connection connection
   * @param catalog a catalog name; must match the catalog name as it
   *        is stored in the database; "" retrieves those without a catalog;
   *        <code>null</code> means that the catalog name should not be used to narrow
   *        the search
   * @param schemaPattern a schema name pattern; must match the schema name
   *        as it is stored in the database; "" retrieves those without a schema;
   *        <code>null</code> means that the schema name should not be used to narrow
   *        the search
   * @param tableNamePattern a table name pattern; must match the
   *        table name as it is stored in the database
   * @param columnNamePattern a column name pattern; must match the column
   *        name as it is stored in the database
   * @return
   */
  List<RawColumn> extractColumn(Connection connection,
                                @Nullable String catalog,
                                @Nullable String schemaPattern,
                                @Nullable String tableNamePattern,
                                @Nullable String columnNamePattern) throws SQLException;

  /**
   *
   * @param catalog a catalog name; must match the catalog name as it
   *        is stored in the database; "" retrieves those without a catalog;
   *        <code>null</code> means that the catalog name should not be used to narrow
   *        the search
   * @param schema a schema name; must match the schema name
   *        as it is stored in the database; "" retrieves those without a schema;
   *        <code>null</code> means that the schema name should not be used to narrow
   *        the search
   * @param tableName a table name; must match the table name as it is stored
   *        in the database
   * @return
   * @throws SQLException
   */
  List<RawPrimaryKey> extractPrimaryKey(Connection connection,
                                        @Nullable String catalog,
                                        @Nullable String schema,
                                        String tableName) throws SQLException;

  /**
   *
   * @param catalog a catalog name; must match the catalog name as it
   *        is stored in this database; "" retrieves those without a catalog;
   *        <code>null</code> means that the catalog name should not be used to narrow
   *        the search
   * @param schema a schema name; must match the schema name
   *        as it is stored in this database; "" retrieves those without a schema;
   *        <code>null</code> means that the schema name should not be used to narrow
   *        the search
   * @param table a table name; must match the table name as it is stored
   *        in this database
   * @param unique when true, return only indices for unique values;
   *     when false, return indices regardless of whether unique or not
   * @param approximate when true, result is allowed to reflect approximate
   *     or out of data values; when false, results are requested to be
   *     accurate
   * @return
   * @throws SQLException
   */
  List<RawIndexInfo> extractIndexInfo(Connection connection,
                                      @Nullable String catalog,
                                      @Nullable String schema,
                                      String table,
                                      boolean unique,
                                      boolean approximate) throws SQLException;


  List<RawForeignKeyInfo> extractExportedKey(Connection connection,
                                             @Nullable String catalog,
                                             @Nullable String schema,
                                             String table) throws SQLException;

  List<RawForeignKeyInfo> extractImportedKey(Connection connection,
                                             @Nullable String catalog,
                                             @Nullable String schema,
                                             String table) throws SQLException;


  List<Object[]> extractArray(Connection connection, String sql) throws SQLException;

  List<Object[]> extractArray(Connection connection, String sql, Object... args) throws SQLException;

  List<Map<String, Object>> extractMap(Connection connection, String sql) throws SQLException;

  List<Map<String, Object>> extractMap(Connection connection, String sql, Object... args) throws SQLException;

  List<Map<String, Object>> explain(Connection connection, String sql) throws SQLException;

  boolean supportRoutine();

  boolean supportDdl();

  enum DDLType{
    TABLE("TABLE"),
    PROCEDURE("PROCEDURE"),
    FUNCTION ("FUNCTION"),
    VIEW ("VIEW"),
    INDEX ("INDEX"),
    TABLESPACE ("TABLESPACE"),
    USER ("USER");

    private String name;

    DDLType(String name) {
      this.name = name;
    }
  }

  String extractDDL(Connection connection, String ddlType, String catalog, String schema, String name) throws SQLException;

  String getVersion(Connection connection) throws SQLException;

  List<RawRoutine> extractRoutine(Connection connection, String type, String catalog, String schema) throws SQLException;

  void close();

}
