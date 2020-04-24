package dev.suntr.extract.parser;

import dev.suntr.extract.entity.RawRoutine;

import java.sql.*;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class MySqlJdbcMetaDataExtractor extends DefaultJdbcMetaDataExtractor {

    private final static Set<String> SYS_CAT = new HashSet<>(Arrays.asList(
            "information_schema",
            "mysql",
            "performance_schema",
            "test",
            "sys"
    ));

    private MySqlJdbcMetaDataExtractor(){}

    public static JdbcMetaDataExtractor newInstance(){
        return new MySqlJdbcMetaDataExtractor();
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
        return metaData.getCatalogs();
    }

    @Override
    public String extractDDL(Connection connection, String ddlType, String catalog, String schema, String name) throws SQLException {
        String sql = "";
        switch (DDLType.valueOf(ddlType)){
            case TABLE:
            case VIEW:
                sql =  "SHOW CREATE " + ddlType + " " + catalog + "." + name;
                break;
            case PROCEDURE:
            case FUNCTION:
                sql = "SELECT * FROM information_schema.routines WHERE routine_name = '" + name +"' AND routine_schema = '" + catalog +"'";
                break;
        }
        try(Statement stmt = connection.createStatement();
            ResultSet resultSet = stmt.executeQuery(sql)){
            while (resultSet.next()){
                return resultSet.getString(2);
            }
        }
        return "";
    }

    private final String ROUTINE_SQL = "SELECT " +
            "db as ROUTINE_CAT, " +
            "null as ROUTINE_SCHEMA, " +
            "name as ROUTINE_NAME, " +
            "type as ROUTINE_TYPE, " +
            "body as ROUTINE_DEFINITION, " +
            "param_list as ROUTINE_PARAMS, " +
            "returns as ROUTINE_RETURNS, " +
            "created as CREATED, " +
            "modified as UPDATED " +
            "FROM `mysql`.`proc` " +
            "WHERE db = '%s'";

    @Override
    public List<RawRoutine> extractRoutine(Connection connection, String type, String catalog, String schema) throws SQLException {
        String sql = String.format(ROUTINE_SQL, catalog);
        if (type!=null && !"".equals(type)){
            sql += " AND type = '" + type + "'";
        }
        PreparedStatement stmt = connection.prepareStatement(sql);
        ResultSet resultSet = stmt.executeQuery();
        return parseToRawRoutine(resultSet);
    }

    @Override
    public boolean supportRoutine() {
        return true;
    }

    @Override
    public boolean supportDdl() {
        return true;
    }
}
// COLUMN1 Table, Column2 Create Table