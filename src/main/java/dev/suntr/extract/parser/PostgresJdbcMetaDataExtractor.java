package dev.suntr.extract.parser;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

public class PostgresJdbcMetaDataExtractor extends DefaultJdbcMetaDataExtractor {

    private PostgresJdbcMetaDataExtractor(){}

    public static JdbcMetaDataExtractor newInstance(){
        return new PostgresJdbcMetaDataExtractor();
    }

    @Override
    protected boolean isSysCatalog(String catalogName) {
        return catalogName!=null && catalogName.toLowerCase().startsWith("pg_");
    }

    @Override
    protected boolean isSysSchema(String schemaName) {
        return schemaName!=null && (schemaName.toLowerCase().startsWith("pg_") || schemaName.equalsIgnoreCase("information_schema"));
    }

    @Override
    protected boolean isSysTable(String tableName) {
        return tableName!=null && tableName.toLowerCase().startsWith("pg_");
    }

    @Override
    protected ResultSet decideDatabase(DatabaseMetaData metaData) throws SQLException {
        return metaData.getCatalogs();
    }
}
