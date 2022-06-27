import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

import org.apache.logging.log4j.*;

public class Connection {
    private String name;
    private String url;
    private String user;
    private String pass;
    private static Logger logger = LogManager.getLogger(Connection.class);

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPass() {
        return pass;
    }

    public void setPass(String pass) {
        this.pass = pass;
    }

    @Override
    public String toString() {
        return this.name + ", " + this.url + ", " + this.user + ", " + this.pass;
    }

    public static List<Connection> yamlParse(String[] paths) {
        Yaml yalm = new Yaml(new Constructor(Connection[].class));
        List<Connection> connectionsList = new LinkedList<Connection>();
        try {
            for (String path : paths) {
                InputStream inputStream = new FileInputStream(path);
                Connection[] connectionsArray = yalm.load(inputStream);
                connectionsList.addAll(Arrays.asList(connectionsArray));
                logger.info("Парсинг файла прошел успешно: " + path);
            }
        } catch (FileNotFoundException e) {
            logger.error("Файл не найден " + e.getMessage());
            System.exit(1);
        }
        logger.info("Парсинг завершён");
        return connectionsList;
    }

//    public static java.sql.Connection conInit(Connection connection) {
//        java.sql.Connection con = null;
//        try {
//            Class.forName("org.postgresql.Driver");
//            con = DriverManager.getConnection("jdbc:postgresql://" + connection.getUrl() + "/" + connection.getName(), connection.getUser(), connection.getPass());
//            logger.info("База данных подключена успешно: " + connection.getName());
//        } catch (SQLException | ClassNotFoundException e) {
//            e.printStackTrace();
//            logger.error("Ошибка подключения к базе данных: " + e.getMessage());
//        }
//        return con;
//    }
    public static HikariDataSource dsInit (Connection connection) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:postgresql://" + connection.getUrl() + "/" + connection.getName());
        config.setUsername(connection.getUser());
        config.setPassword(connection.getPass());
        config.addDataSourceProperty( "cachePrepStmts" , "true" );
        config.addDataSourceProperty( "prepStmtCacheSize" , "250" );
        config.addDataSourceProperty( "prepStmtCacheSqlLimit" , "2048" );
        logger.info("DataSource инициализирован");
        return new HikariDataSource(config);
    }
    public static java.sql.Connection getConnection(HikariDataSource dataSource) throws SQLException {
        logger.info("Соединение получено");
        return dataSource.getConnection();
    }
}
