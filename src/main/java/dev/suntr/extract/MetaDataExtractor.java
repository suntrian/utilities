package dev.suntr.extract;

import com.quantchi.jdbcextractor.entity.*;

import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.List;

public interface MetaDataExtractor {

  /**
   * TABLE_CAT
   */
  List<RawDatabase> extractDatabase() throws SQLException;

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
   */
  List<RawTable> extractTable(@Nullable String catalog,
                              @Nullable String schemaPattern,
                              @Nullable String tableNamePattern,
                              @Nullable String... types) throws SQLException;

  List<RawTable> extractTable() throws SQLException;
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
   */
  List<RawColumn> extractColumn(@Nullable String catalog,
                                @Nullable String schemaPattern,
                                @Nullable String tableNamePattern,
                                @Nullable String columnNamePattern) throws SQLException;

  List<RawPrimaryKey> extractPrimaryKey(@Nullable String catalog,
                                        @Nullable String schema,
                                        String tableName) throws SQLException;

  List<RawIndexInfo> extractIndexInfo(@Nullable String catalog,
                                      @Nullable String schema,
                                      String tableName,
                                      boolean unique,
                                      boolean approximate) throws SQLException;

  List<RawForeignKeyInfo> extractExportedKey(@Nullable String catalog,
                                                @Nullable String schema,
                                                String table) throws SQLException;

  List<RawForeignKeyInfo> extractImportedKey(@Nullable String catalog,
                                             @Nullable String schema,
                                             String table) throws SQLException;

  List<RawRoutine> extractRoutine(String catalog,
                                  String schema,
                                  String type) throws SQLException;


  String getVersion() throws SQLException;

  String getTableDDL(String catalog, String schema, String table) throws SQLException;

  boolean supportRoutine();

  boolean supportDdl();
}
