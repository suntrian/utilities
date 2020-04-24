package dev.suntr.extract.datasource;

import dev.suntr.extract.exception.UnSupportedJdbcTypeException;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public enum DriverType {
    //todo 检查不同版本的服务器与驱动之间的兼容性
    MYSQL("com.mysql.cj.jdbc.Driver","jdbc:mysql://${host}:${port}/${database}", 3306),
    /**
     * oracle service name 连接 jdbc:oracle:thin:@//${host}:${port}/${database}
     */
    ORACLE("oracle.jdbc.driver.OracleDriver","jdbc:oracle:thin:@${host}:${port}:${database}", 1521),
    /**
     * jdbc连接DB2主要为两种方式
     * Type2：url形式如 ：jdbc:db2:DBName,需要依赖本地库文件。
     * Type4：url形式如：jdbc:db2://ip:port/DBName,与Type2形式的主要区别也就是多了IP/Port这种直连形式，纯java实现的，无需依赖本地库文件
     */
    DB2("com.ibm.db2.jcc.DB2Driver","jdbc:db2://${host}:${port}/${database}",50000),
    SQLSERVER("com.microsoft.sqlserver.jdbc.SQLServerDriver","jdbc:sqlserver://${host}:${port};DatabaseName=${database}",1433),
    POSTGRESQL("org.postgresql.Driver","jdbc:postgresql://${host}:${port}/${database}",5432),
    SYBASE("com.sybase.jdbc.SybDriver","jdbc:sybase:Tds:${host}:${port}/${database}",5000),
    HIVE("org.apache.hive.jdbc.HiveDriver","jdbc:hive2://${host}:${port}/${database}",10000),
    IMPALA("com.cloudera.impala.jdbc41.Driver","jdbc:impala://${host}:${port}/${database}",21050),
    ODBC("sun.jdbc.odbc.JdbcOdbcDriver","jdbc:odbc:${database}",0);

    private String driver;

    private String pattern;

    private Integer port;

    DriverType(String driver, String pattern, Integer port) {
        this.driver = driver;
        this.pattern = pattern;
        this.port = port;
    }

    public static DriverType judgeDriver(String url){
        url = url.trim().toLowerCase();
        if (url.startsWith("jdbc")){
            Pattern jdbcPat = Pattern.compile("jdbc:(\\S+?):");
            Matcher matcher = jdbcPat.matcher(url);
            if (matcher.find()){
                String driver = matcher.group(1);
                switch (driver.toLowerCase()){
                    case "mysql":
                        return MYSQL;
                    case "oracle":
                        return ORACLE;
                    case "sybase":
                        return SYBASE;
                    case "microsoft":
                    case "sqlserver":
                        return SQLSERVER;
                    case "db2":
                        return DB2;
                    case "postgresql":
                        return POSTGRESQL;
                    case "hive":
                    case "hive2":
                        return HIVE;
                    case "impala":
                        return IMPALA;
                }
            }
        }
        throw new UnSupportedJdbcTypeException(url + " is not a valid jdbc connect string");
    }

    public String getDriver() {
        return driver;
    }

    public String getPattern() {
        return pattern;
    }

    public Integer getPort() {
        return port;
    }

    public static String getUrl(DriverType driver, String host, Integer port, String db){
        String url = driver.getPattern();
        url = url.replace("${host}", host);
        url = url.replace("${port}", checkPortValid(port)? port.toString(): driver.getPort().toString());
        url = url.replace("${database}", db==null?"":db);
        return url;
    }

    private static boolean checkPortValid(Integer port){
        if (port==null){return false;}
        return port>=1024 && port <= 49151;
    }

    private static final Pattern MYSQL_PATTERN = Pattern.compile("^jdbc:(?<proto>mysql)://(?<host>[^/\\: ]+)(?::(?<port>\\d{4,5}))?(?:/(?<database>\\S*?))?(?:\\?(?<params>\\S+?=\\S+?)*&?)?$", Pattern.CASE_INSENSITIVE);
    private static final Pattern ORACLE_PATTERN = Pattern.compile("^jdbc:(?<proto>oracle):(?<proc>\\S+?):@(?://)?(?<host>[^/\\: ]+):(?<port>\\d{4,5})[/:](?<database>\\S*)$", Pattern.CASE_INSENSITIVE);
    private static final Pattern MSSQL_PATTERN = Pattern.compile("^jdbc:(?:microsoft:)?(?<proto>sqlserver)://(?<host>[^/\\:; ]+)(?::(?<port>\\d{4,5}))?(?:;|;databasename=(?<database>\\S*))?$", Pattern.CASE_INSENSITIVE);
    private static final Pattern HIVE_PATTERN = Pattern.compile("^jdbc:(?<proto>hive2|impala)://(?<host>[^/\\: ]+)(?::(?<port>\\d{4,5}))?(?:/|/(?<database>\\S*?))?(?:\\?(?<params>\\S+?=\\S+?)*&?)?$", Pattern.CASE_INSENSITIVE);
    private static final Pattern POSTGRE_PATTERN = Pattern.compile("^jdbc:(?<proto>postgresql)://(?<host>[^/\\: ]+)(?::(?<port>\\d{4,5}))?(?:/(?<database>\\S*?))?(?:\\?(?<params>\\S+?=\\S+?)*&?)?$", Pattern.CASE_INSENSITIVE);
    private static final Pattern DB2_PATTERN = Pattern.compile("^jdbc:(?<proto>db2)://(?<host>[^/\\: ]+)(?::(?<port>\\d{4,5}))?(?:/(?<database>\\S*?))?(?:\\?(?<params>\\S+?=\\S+?)*&?)?$", Pattern.CASE_INSENSITIVE);

    public static DataSource parseProperties(String jdbcUrl){
        DriverType driver = judgeDriver(jdbcUrl);
        Matcher matcher;
        switch (driver){
            case IMPALA:
            case HIVE:
                matcher = HIVE_PATTERN.matcher(jdbcUrl);
                break;
            case SQLSERVER:
                matcher = MSSQL_PATTERN.matcher(jdbcUrl);
                break;
            case ORACLE:
                matcher = ORACLE_PATTERN.matcher(jdbcUrl);
                break;
            case MYSQL:
                matcher = MYSQL_PATTERN.matcher(jdbcUrl);
                break;
            case POSTGRESQL:
                matcher = POSTGRE_PATTERN.matcher(jdbcUrl);
                break;
            case DB2:
                matcher = DB2_PATTERN.matcher(jdbcUrl);
                break;
            case SYBASE:
            case ODBC:
            default:
                throw new UnSupportedJdbcTypeException("unsupported yet");
        }
        DataSource dataSource = new DataSource();
        if (matcher.find()){
            dataSource.setProtocol(matcher.group("proto"));
            dataSource.setHost(matcher.group("host"));
            if (matcher.group("port") == null){
                dataSource.setPort(driver.getPort());
            } else {
                dataSource.setPort(Integer.valueOf(matcher.group("port")));
            }
            dataSource.setDatabase(matcher.group("database"));
        }
        return dataSource;
    }

    public static class DataSource{
        private String protocol;
        private String host;
        private String database;
        private Integer port;

        public String getProtocol() {
            return protocol;
        }

        public void setProtocol(String protocol) {
            this.protocol = protocol;
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public String getDatabase() {
            return database;
        }

        public void setDatabase(String database) {
            this.database = database;
        }

        public Integer getPort() {
            return port;
        }

        public void setPort(Integer port) {
            this.port = port;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DataSource that = (DataSource) o;
            return Objects.equals(protocol, that.protocol) &&
                    Objects.equals(host, that.host) &&
                    Objects.equals(database, that.database) &&
                    Objects.equals(port, that.port);
        }

        @Override
        public int hashCode() {
            return Objects.hash(protocol, host, database, port);
        }
    }

}

