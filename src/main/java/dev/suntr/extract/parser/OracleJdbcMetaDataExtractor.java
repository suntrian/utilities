package dev.suntr.extract.parser;

import com.sun.istack.internal.Nullable;
import dev.suntr.extract.datasource.DatasourceResolver;
import dev.suntr.extract.entity.RawColumn;
import dev.suntr.extract.entity.RawForeignKeyInfo;
import dev.suntr.extract.entity.RawIndexInfo;
import dev.suntr.extract.entity.RawPrimaryKey;
import oracle.jdbc.driver.DatabaseError;
import oracle.jdbc.driver.OracleSql;
import oracle.jdbc.internal.OracleConnection;
import oracle.jdbc.internal.OracleResultSet;

import java.sql.*;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@SuppressWarnings("Duplicates")
public class OracleJdbcMetaDataExtractor extends DefaultJdbcMetaDataExtractor {

    private static final List<String> SYS_SCHEMA = Arrays.asList(
            "ANONYMOUS",
            "APEX_030200",
            "APEX_PUBLIC_USER",
            "APPQOSSYS",
            "CTXSYS",
            "DBSNMP",
            "DIP",
            "EXFSYS",
            "FLOWS_FILES",
            "MDDATA",
            "MDSYS",
            "MGMT_VIEW",
            "OLAPSYS",
            "ORACLE_OCM",
            "ORDDATA",
            "ORDPLUGINS",
            "ORDSYS",
            "OUTLN",
            "OWBSYS",
            "OWBSYS_AUDIT",
            "SCOTT",
            "SI_INFORMTN_SCHEMA",
            "SPATIAL_CSW_ADMIN_USR",
            "SPATIAL_WFS_ADMIN_USR",
            "SYS",
            "SDQN",
            "SYSMAN",
            "SYSTEM",
            "TMR",
            "WKPROXY",
            "WMSYS",
            "XDB",
            "XS$NULL");
    protected OracleJdbcMetaDataExtractor(){}

    public static JdbcMetaDataExtractor newInstance(){
        return new OracleJdbcMetaDataExtractor();
    }

    @Override
    protected boolean isSysCatalog(String catalogName) {
        return catalogName!=null && SYS_SCHEMA.contains(catalogName.toUpperCase());
    }

    @Override
    protected boolean isSysSchema(String schemaName) {
        return schemaName!=null && SYS_SCHEMA.contains( schemaName.toUpperCase());
    }

    @Override
    protected boolean isSysTable(String tableName) {
        return false;
    }

    @Override
    protected ResultSet decideDatabase(DatabaseMetaData metaData) throws SQLException {
        return metaData.getSchemas();
    }

    @Override
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
            if (Integer.MAX_VALUE == column.getCOLUMN_SIZE()) {
                column.setCOLUMN_SIZE(null);
            }
            //column.setBUFFER_LENGTH(resultSet.getInt(8 /*"BUFFER_LENGTH"*/));
            if (isFloatingNumber(column.getTYPE_NAME())) {
                column.setDECIMAL_DIGITS(resultSet.getInt(9 /*"DECIMAL_DIGITS"*/));
                if (0 == column.getCOLUMN_SIZE() && -127 == column.getDECIMAL_DIGITS()) {
                    //oracle定义NUMBER类型字段，允许NUMBER, NUMBER(12), NUMBER(*, 4), NUMBER(12, 4)等等不同写法，以下针对第一种写法进行处理。
                    column.setCOLUMN_SIZE(null);
                    column.setDECIMAL_DIGITS(null);
                }
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

    private PreparedStatement primaryKeyStatement;

    @Override
    public List<Map<String, Object>> explain(Connection connection, String sql) throws SQLException{
        String explainSql = "EXPLAIN PLAN FOR " + sql;
        return extractMap(connection, explainSql);
    }

    @Override
    public List<RawPrimaryKey> extractPrimaryKey(Connection connection, @Nullable String catalog, @Nullable String schema, String tableName) throws SQLException {
        if (primaryKeyStatement == null){
            primaryKeyStatement = connection.prepareStatement("SELECT NULL AS table_cat,\n       c.owner AS table_schem,\n       c.table_name,\n       c.column_name,\n       c.position AS key_seq,\n       c.constraint_name AS pk_name\nFROM all_cons_columns c, all_constraints k\nWHERE k.constraint_type = 'P'\n  AND k.table_name = :1\n  AND k.owner like :2 escape '/'\n  AND k.constraint_name = c.constraint_name \n  AND k.table_name = c.table_name \n  AND k.owner = c.owner \nORDER BY column_name\n");
        }
        primaryKeyStatement.setString(1, tableName);
        primaryKeyStatement.setString(2, schema == null ? "%" : schema);
        //primaryKeyStatement.closeOnCompletion();
        OracleResultSet resultSet = (OracleResultSet)primaryKeyStatement.executeQuery();
        return parseToPrimaryKey(resultSet);
    }

  /**
   * checked from ojdbc14,  while ojdbc8 is newer and apply for jdk8+,
   */
/*
    @Override
    public List<RawIndexInfo> extractIndexInfo(Connection connection, @Nullable String catalog, @Nullable String schema, String table, boolean unique, boolean approximate) throws SQLException {
        Statement var6 = connection.createStatement();
        if (schema != null && schema.length() != 0 && !OracleSql.isValidObjectName(schema) || table != null && table.length() != 0 && !OracleSql.isValidObjectName(table)) {
            DatabaseError.throwSqlException(68);
        }

        String var7;
        if (!approximate) {
            var7 = "analyze table " + (schema == null ? "" : schema + ".") + table + " compute statistics";
            var6.executeUpdate(var7);
        }

        var7 = "select null as table_cat,\n       owner as table_schem,\n       table_name,\n       0 as NON_UNIQUE,\n       null as index_qualifier,\n       null as index_name, 0 as type,\n       0 as ordinal_position, null as column_name,\n       null as asc_or_desc,\n       num_rows as cardinality,\n       blocks as pages,\n       null as filter_condition\nfrom all_tables\nwhere table_name = '" + table + "'\n";
        String var8 = "";
        if (schema != null && schema.length() > 0) {
            var8 = "  and owner = '" + schema + "'\n";
        }

        String var9 = "select null as table_cat,\n       i.owner as table_schem,\n       i.table_name,\n       decode (i.uniqueness, 'UNIQUE', 0, 1),\n       null as index_qualifier,\n       i.index_name,\n       1 as type,\n       c.column_position as ordinal_position,\n       c.column_name,\n       null as asc_or_desc,\n       i.distinct_keys as cardinality,\n       i.leaf_blocks as pages,\n       null as filter_condition\nfrom all_indexes i, all_ind_columns c\nwhere i.table_name = '" + table + "'\n";
        String var10 = "";
        if (schema != null && schema.length() > 0) {
            var10 = "  and i.owner = '" + schema + "'\n";
        }

        String var11 = "";
        if (unique) {
            var11 = "  and i.uniqueness = 'UNIQUE'\n";
        }
        String var12 = "  and i.index_name = c.index_name\n  and i.table_owner = c.table_owner\n  and i.table_name = c.table_name\n  and i.owner = c.index_owner\n";
        String var13 = "order by non_unique, type, index_name, ordinal_position\n";
        String var14 = var7 + var8 + "union\n" + var9 + var10 + var11 + var12 + var13;
        OracleResultSet var15 = (OracleResultSet)var6.executeQuery(var14);
        // var15.closeStatementOnClose();
        List<RawIndexInfo> rawIndexInfos = parseToIndexInfo(var15);
        return rawIndexInfos;
    }
*/

    /**
     * checked from ojdbc8, not work in ojdbc14 apply for jdk1.4+ , while ojdbc8 is newer and apply for jdk8+,
     * @param connection
     * @param catalog
     * @param schema
     * @param table
     * @return
     * @throws SQLException
     */
    @Override
    public List<RawIndexInfo> extractIndexInfo(Connection connection, @Nullable String catalog, @Nullable String schema, String table, boolean unique, boolean approximate) throws SQLException {
        Statement var6 = connection.createStatement();
        if ((schema == null || schema.length() == 0 || OracleSql.isValidObjectName(schema)) && (table == null || table.length() == 0 || OracleSql.isValidObjectName(table))) {
            String var8;
            if (!approximate) {
                boolean var7 = false;

                try {
                    var7 = ((OracleConnection) connection).getTransactionState().contains(OracleConnection.TransactionState.TRANSACTION_STARTED);
                } catch (SQLException var16) {
                }

                if (!var7) {
                    var8 = "analyze table " + (schema == null ? "" : schema + ".") + table + " compute statistics";
                    var6.executeUpdate(var8);
                }
            }

            String var17 = "select null as table_cat,\n       owner as table_schem,\n       table_name,\n       0 as NON_UNIQUE,\n       null as index_qualifier,\n       null as index_name, 0 as type,\n       0 as ordinal_position, null as column_name,\n       null as asc_or_desc,\n       num_rows as cardinality,\n       blocks as pages,\n       null as filter_condition\nfrom all_tables\nwhere table_name = " + quoteDatabaseObjectName(table) + "\n";
            var8 = "";
            if (schema != null && schema.length() > 0) {
                var8 = "  and owner = " + quoteDatabaseObjectName(schema) + "\n";
            }

            String var9 = "select null as table_cat,\n       i.owner as table_schem,\n       i.table_name,\n       decode (i.uniqueness, 'UNIQUE', 0, 1),\n       null as index_qualifier,\n       i.index_name,\n       1 as type,\n       c.column_position as ordinal_position,\n       c.column_name,\n       null as asc_or_desc,\n       i.distinct_keys as cardinality,\n       i.leaf_blocks as pages,\n       null as filter_condition\nfrom all_indexes i, all_ind_columns c\nwhere i.table_name = " + quoteDatabaseObjectName(table) + "\n";
            String var10 = "";
            if (schema != null && schema.length() > 0) {
                var10 = "  and i.owner = " + quoteDatabaseObjectName(schema) + "\n";
            }

            String var11 = "";
            if (unique) {
                var11 = "  and i.uniqueness = 'UNIQUE'\n";
            }

            String var12 = "  and i.index_name = c.index_name\n  and i.table_owner = c.table_owner\n  and i.table_name = c.table_name\n  and i.owner = c.index_owner\n";
            String var13 = "order by non_unique, type, index_name, ordinal_position\n";
            String var14 = var17 + var8 + "union\n" + var9 + var10 + var11 + var12 + var13;
            var6.closeOnCompletion();
            OracleResultSet var15 = (OracleResultSet)var6.executeQuery(var14);
            List<RawIndexInfo> rawIndexInfos = parseToIndexInfo(var15);
            return rawIndexInfos;
        } else {
            throw (SQLException)((SQLException) DatabaseError.createSqlException(68).fillInStackTrace());
        }
    }

    @Override
    public List<RawForeignKeyInfo> extractExportedKey(Connection connection, @Nullable String catalog, @Nullable String schema, String table) throws SQLException {
        ResultSet resultSet = this.keys_query(connection, schema, table, (String)null, (String)null, "ORDER BY fktable_schem, fktable_name, key_seq");
        return parseToForeignKeyInfo(resultSet);
    }

    @Override
    public List<RawForeignKeyInfo> extractImportedKey(Connection connection, @Nullable String catalog, @Nullable String schema, String table) throws SQLException {
        ResultSet resultSet = this.keys_query(connection, (String)null, (String)null, schema, table, "ORDER BY pktable_schem, pktable_name, key_seq");
        return parseToForeignKeyInfo(resultSet);
    }

    @Override
    public boolean supportDdl() {
        return true;
    }

    @Override
    public String extractDDL(Connection connection, String ddlType, String catalog, String schema, String name) throws SQLException {
        String ddlSql =  "SELECT dbms_metadata.get_ddl( '" + ddlType + "', '" + name + "', '"+ schema +"' ) FROM DUAL";
        try ( Statement stmt = connection.createStatement();
              ResultSet resultSet = stmt.executeQuery(ddlSql)) {
            StringBuilder builder = new StringBuilder();
            while (resultSet.next()){
                builder.append(resultSet.getString(1));
            }
            return  builder.toString();
        }
    }

    private PreparedStatement keyQueryStatement;

    private ResultSet keys_query(Connection connection, String var1, String var2, String var3, String var4, String approximate) throws SQLException {
        int var6 = 1;
        int var7 = var2 != null ? var6++ : 0;
        int var8 = var4 != null ? var6++ : 0;
        int var9 = var1 != null && var1.length() > 0 ? var6++ : 0;
        int var10 = var3 != null && var3.length() > 0 ? var6++ : 0;

        if (keyQueryStatement == null){
            keyQueryStatement = connection.prepareStatement("SELECT NULL AS pktable_cat,\n       p.owner as pktable_schem,\n       p.table_name as pktable_name,\n       pc.column_name as pkcolumn_name,\n       NULL as fktable_cat,\n       f.owner as fktable_schem,\n       f.table_name as fktable_name,\n       fc.column_name as fkcolumn_name,\n       fc.position as key_seq,\n       NULL as update_rule,\n       decode (f.delete_rule, 'CASCADE', 0, 'SET NULL', 2, 1) as delete_rule,\n       f.constraint_name as fk_name,\n       p.constraint_name as pk_name,\n       decode(f.deferrable,       'DEFERRABLE',5      ,'NOT DEFERRABLE',7      , 'DEFERRED', 6      ) deferrability \n      FROM all_cons_columns pc, all_constraints p,\n      all_cons_columns fc, all_constraints f\nWHERE 1 = 1\n" + (var7 != 0 ? "  AND p.table_name = :1\n" : "") + (var8 != 0 ? "  AND f.table_name = :2\n" : "") + (var9 != 0 ? "  AND p.owner = :3\n" : "") + (var10 != 0 ? "  AND f.owner = :4\n" : "") + "  AND f.constraint_type = 'R'\n" + "  AND p.owner = f.r_owner\n" + "  AND p.constraint_name = f.r_constraint_name\n" + "  AND p.constraint_type = 'P'\n" + "  AND pc.owner = p.owner\n" + "  AND pc.constraint_name = p.constraint_name\n" + "  AND pc.table_name = p.table_name\n" + "  AND fc.owner = f.owner\n" + "  AND fc.constraint_name = f.constraint_name\n" + "  AND fc.table_name = f.table_name\n" + "  AND fc.position = pc.position\n" + approximate);
        }
        if (var7 != 0) {
            keyQueryStatement.setString(var7, var2);
        }
        if (var8 != 0) {
            keyQueryStatement.setString(var8, var4);
        }
        if (var9 != 0) {
            keyQueryStatement.setString(var9, var1);
        }
        if (var10 != 0) {
            keyQueryStatement.setString(var10, var3);
        }
        //keyQueryStatement.closeOnCompletion();
        OracleResultSet var12 = (OracleResultSet)keyQueryStatement.executeQuery();
        return var12;
    }

    private static final String quoteDatabaseObjectName(String var0) throws SQLException {
        if (var0 != null && var0.length() != 0) {
            assert OracleSql.isValidObjectName(var0) : "n is invalid \"" + var0 + "\"";

            return (var0.charAt(0) == '"' ? "Q'" : "'") + var0 + "'";
        } else {
            return "''";
        }
    }

    @Override
    public void close() {
        try {
            if (this.primaryKeyStatement!=null){
                this.primaryKeyStatement.close();
            }
        } catch (SQLException e) {}
        finally {
            this.primaryKeyStatement = null;
        }
        try {
            if (this.keyQueryStatement!=null){
                this.keyQueryStatement.close();
            }
        } catch (SQLException e) {}
        finally {
            this.keyQueryStatement = null;
        }
        super.close();
    }
}
