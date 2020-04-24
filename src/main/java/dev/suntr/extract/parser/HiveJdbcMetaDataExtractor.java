package dev.suntr.extract.parser;

import com.sun.istack.internal.Nullable;
import dev.suntr.extract.datasource.DatasourceResolver;
import dev.suntr.extract.entity.RawColumn;
import dev.suntr.extract.entity.RawForeignKeyInfo;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;

@SuppressWarnings("all")
public class HiveJdbcMetaDataExtractor extends DefaultJdbcMetaDataExtractor {

    private HiveJdbcMetaDataExtractor(){}

    public static JdbcMetaDataExtractor newInstance(){
        return new HiveJdbcMetaDataExtractor();
    }

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
        return metaData.getSchemas();
    }

    @Override
    protected List<RawColumn> parseToColumn(ResultSet resultSet) throws SQLException {
        List<RawColumn> rawColumns = new LinkedList<>();
        while (resultSet.next()){
            RawColumn column = new RawColumn();
            rawColumns.add(column);
            column.setTABLE_CAT(resultSet.getString(1 /*"TABLE_CAT"*/));
            column.setTABLE_SCHEM(resultSet.getString(2 /*"TABLE_SCHEM"*/));
            column.setTABLE_NAME(resultSet.getString(3 /*"TABLE_NAME"*/));
            column.setCOLUMN_NAME(resultSet.getString(4 /*"COLUMN_NAME"*/));
            column.setDATA_TYPE(resultSet.getInt(5 /*"DATA_TYPE"*/));
            column.setTYPE_NAME(resultSet.getString(6 /*"TYPE_NAME"*/));
            column.setCOLUMN_SIZE(resultSet.getInt(7 /*"COLUMN_SIZE"*/));
            if (Integer.MAX_VALUE == column.getCOLUMN_SIZE()) {
                column.setCOLUMN_SIZE(null);
            }
            column.setBUFFER_LENGTH(resultSet.getInt(8 /*"BUFFER_LENGTH"*/));
            if (isFloatingNumber(column.getTYPE_NAME())){
                column.setDECIMAL_DIGITS(resultSet.getInt(9 /*"DECIMAL_DIGITS"*/));
            }
            column.setNUM_PREC_RADIX(resultSet.getInt(10 /*"NUM_PREC_RADIX"*/));
            column.setNULLABLE(resultSet.getInt(11 /*"NULLABLE"*/));
            column.setREMARKS(resultSet.getString(12 /*"REMARKS"*/));
            column.setCOLUMN_DEF(resultSet.getString(13 /*"COLUMN_DEF"*/));
            column.setSQL_DATA_TYPE(resultSet.getInt(14 /*"SQL_DATA_TYPE"*/));
            column.setSQL_DATETIME_SUB(resultSet.getInt(15 /*"SQL_DATETIME_SUB"*/));
            column.setCHAR_OCTET_LENGTH(resultSet.getInt(16 /*"CHAR_OCTET_LENGTH"*/));
            column.setORDINAL_POSITION(resultSet.getInt(17 /*"ORDINAL_POSITION"*/));
            column.setIS_NULLABLE(resultSet.getString(18 /*"IS_NULLABLE"*/));
            column.setSCOPE_CATALOG(resultSet.getString(19 /*"SCOPE_CATALOG"*/));
            column.setSCOPE_SCHEMA(resultSet.getString(20 /*"SCOPE_SCHEMA"*/));
            column.setSCOPE_TABLE(resultSet.getString(21 /*"SCOPE_TABLE"*/));
            column.setSOURCE_DATA_TYPE(resultSet.getShort(22 /*"SOURCE_DATA_TYPE"*/));
            column.setIS_AUTOINCREMENT(resultSet.getString(23 /*"IS_AUTO_INCREMENT"*/));
        }
        DatasourceResolver.close(resultSet);
        return rawColumns;
    }

    @Override
    public List<RawForeignKeyInfo> extractExportedKey(Connection connection, @Nullable String catalog, @Nullable String schema, String table) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<RawForeignKeyInfo> extractImportedKey(Connection connection, @Nullable String catalog, @Nullable String schema, String table) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String extractDDL(Connection connection, String ddlType, String catalog, String schema, String name) throws SQLException {
        String sql = null;
        String database = schema == null? catalog: schema;
        switch (DDLType.valueOf(ddlType)){
            case TABLE:
            case VIEW:
                sql =  "SHOW CREATE " + ddlType + " " + (database==null||"".equals(database)? "": database + ".") + name;
                break;
        }
        if (sql == null) {return "";}
        try(Statement stmt = connection.createStatement();
            ResultSet resultSet = stmt.executeQuery(sql)){
            StringBuilder builder = new StringBuilder();
            while (resultSet.next()){
                builder.append(resultSet.getString(1));
            }
            return builder.toString();
        }
    }

    @Override
    public boolean supportDdl() {
        return true;
    }
}
