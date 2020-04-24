package dev.suntr.extract;

import com.quantchi.jdbcextractor.datasource.DatasourceResolver;
import com.quantchi.jdbcextractor.datasource.DriverType;
import com.quantchi.jdbcextractor.entity.*;
import com.quantchi.jdbcextractor.parser.*;
import lombok.Getter;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class JDBCExtractor implements MetaDataExtractor, com.quantchi.jdbcextractor.SqlExtractor, Closeable {

    @Getter
    private Connection connection;

    private DriverType driverType;

    private JdbcMetaDataExtractor extractor;

    public JDBCExtractor(String url, String username, String password) throws SQLException, ClassNotFoundException {
        this.driverType = DriverType.judgeDriver(url);
        this.connection = DatasourceResolver.getConnection(url, username, password);
        this.extractor = determineExtractor(driverType);
    }

    public JDBCExtractor(DriverType driver, String host, Integer port, String database, String username, String password)
            throws SQLException, ClassNotFoundException {
        this.driverType = driver;
        this.connection = DatasourceResolver.getConnection(driver, host, port, database, username, password);
        this.extractor = determineExtractor(driverType);
    }

    /**
     * mysql 只有catalog,没有schema
     * oracle 只有schema 没有catalog
     * sqlserver、postgresql 有 catalog和schema，catalog作为数据库
     *
     * @return {@link RawDatabase}
     * @throws SQLException just throw out
     */
    @Override
    public List<RawDatabase> extractDatabase() throws SQLException {
        return this.extractor.extractDatabase(this.connection);
    }

    @Override
    public List<RawTable> extractTable() throws SQLException {
        return extractTable( null, null, null,"TABLE","VIEW");
    }

    @Override
    public List<RawTable> extractTable(@Nullable String catalog,
                                       @Nullable String schemaPattern,
                                       @Nullable String tableNamePattern,
                                       @Nullable String... types) throws SQLException {
        return this.extractor.extractTable(connection, catalog, schemaPattern, tableNamePattern, types);
    }

    public List<RawColumn> extractColumn() throws SQLException {
        return extractColumn(null, null, null, null);
    }

    @Override
    public List<RawColumn> extractColumn(@Nullable String catalog,
                                         @Nullable String schemaPattern,
                                         @Nullable String tableNamePattern,
                                         @Nullable String columnNamePattern) throws SQLException {
        return this.extractor.extractColumn(connection, catalog, schemaPattern, tableNamePattern, columnNamePattern);
    }

    public List<RawPrimaryKey> extractPrimaryKey(String tableName) throws SQLException {
        return extractPrimaryKey(null, null, tableName);
    }

    @Override
    public List<RawPrimaryKey> extractPrimaryKey(@Nullable String catalog,
                                                 @Nullable String schema,
                                                 String tableName) throws SQLException {
        return this.extractor.extractPrimaryKey(connection, catalog, schema, tableName);
    }

    public List<RawIndexInfo> extractIndexInfo(String tableName,
                                               boolean unique,
                                               boolean approximate) throws SQLException {
        return extractIndexInfo(null, null, tableName, unique, approximate);
    }

    @Override
    public List<RawIndexInfo> extractIndexInfo(@Nullable String catalog,
                                               @Nullable String schema,
                                               String tableName,
                                               boolean unique,
                                               boolean approximate) throws SQLException {
        return this.extractor.extractIndexInfo(connection, catalog, schema, tableName, unique, approximate);
    }

    @Override
    public List<RawForeignKeyInfo> extractExportedKey(@Nullable String catalog,
                                                      @Nullable String schema,
                                                      String table) throws SQLException {
        return this.extractor.extractExportedKey(this.connection, catalog, schema, table);
    }

    @Override
    public List<RawForeignKeyInfo> extractImportedKey(@Nullable String catalog,
                                                      @Nullable String schema,
                                                      String table) throws SQLException {
        return this.extractor.extractImportedKey(this.connection, catalog, schema, table);
    }

    @Override
    public List<RawRoutine> extractRoutine(String catalog,
                                           String schema,
                                           String type) throws SQLException {
        return this.extractor.extractRoutine(this.connection,type,  catalog, schema);
    }

    @Override
    public List<Object[]> extractArray(String sql) throws SQLException {
        return this.extractor.extractArray(this.connection, sql);
    }

    @Override
    public List<Object[]> extractArray(String sql, Object... args) throws SQLException {
        return this.extractor.extractArray(this.connection, sql, args);
    }

    @Override
    public List<Map<String, Object>> extractMap(String sql) throws SQLException {
        return this.extractor.extractMap(this.connection, sql);
    }

    @Override
    public List<Map<String, Object>> extractMap(String sql, Object... args) throws SQLException {
        return this.extractor.extractMap(this.connection, sql, args);
    }

    @Override
    public List<Map<String, Object>> explain( String sql) throws SQLException {
        return this.extractor.explain(this.connection, sql);
    }

    @Override
    public String getVersion() throws SQLException {
        return this.extractor.getVersion(this.connection);
    }

    @Override
    public String getTableDDL(String catalog, String schema, String table) throws SQLException {
        return this.extractor.extractDDL(this.connection, JdbcMetaDataExtractor.DDLType.TABLE.name(), catalog, schema, table);
    }

    @Override
    public boolean supportRoutine() {
        return this.extractor.supportRoutine();
    }

    @Override
    public boolean supportDdl() {
        return this.extractor.supportDdl();
    }

    public void close(){
        if (this.extractor!=null){
            this.extractor.close();
        }
        DatasourceResolver.close(this.connection);
    }

    @Override
    protected void finalize() {
        close();
    }

    private JdbcMetaDataExtractor determineExtractor(DriverType driverType){
        if (driverType == null){
            return DefaultJdbcMetaDataExtractor.newInstance();
        }
        switch (driverType){
            case ORACLE:
                return OracleJdbcMetaDataExtractor.newInstance();
            case SQLSERVER:
                return SQLServerJdbcMetaDataExtractor.newInstance();
            case MYSQL:
                return MySqlJdbcMetaDataExtractor.newInstance();
            case POSTGRESQL:
                return PostgresJdbcMetaDataExtractor.newInstance();
            case DB2:
                return DB2JdbcMetaDataExtractor.newInstance();
            case HIVE:
            case IMPALA:
                return HiveJdbcMetaDataExtractor.newInstance();
            case SYBASE:
            case ODBC:
            default:
                return DefaultJdbcMetaDataExtractor.newInstance();
        }
    }

}
