import com.zaxxer.hikari.HikariDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.management.*;
import java.lang.management.ManagementFactory;
import java.sql.*;
import java.util.*;
import java.sql.PreparedStatement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Main extends Thread {
    private static final Logger logger = LogManager.getLogger(Main.class);

    //TODO отрефакторить код
    //TODO добавить потоки ExecutorService погуглить почитать
    //TODO переделать парсер чтобы набивал хешмапу а не список объектов
    //TODO прописать получение конекшенов из хеш-мапы после парса
    //TODO
    //TODO

    public static void main(String[] args) throws SQLException {
        if (args.length == 0) {
            logger.error("Отсутствует путь до файла конфигурации");
            return;
            //  throw new IllegalArgumentException("Отсутствует путь до файла конфигурации");
        }
        List<Connection> conList = Parser.yamlParse(args);
        List<HikariDataSource> dsList = Connection.dataSourceList(conList);
        List<java.sql.Connection> javaConList = Connection.javaConList(dsList);
        List<String> tableNames = new ArrayList<String>() {{ //TODO временное решение пока не прописан парсер
            add("students");
            add("clients");
        }};
        Map<String, List<String>> tableStructure = DB.getTableStructure(javaConList.get(0), tableNames);
        java.sql.Connection con1 = Connection.getConnection(dsList, 0);
        java.sql.Connection con2 = Connection.getConnection(dsList, 1);
        //System.out.println(String.join("\n", getTableContent(con1, tableStructure)));
        replicate(con1, con2, tableStructure);
        testJmx(javaConList);
    }

    public static List<String> getTableContent(java.sql.Connection connection, Map<String, List<String>> tableStructure) {
        List<String> tableContentList = new ArrayList<>();
        PreparedStatement select;
        for (String tableName : tableStructure.keySet()) {
            String columns = Parser.listToString(tableStructure.get(tableName));
            try {
                select = connection.prepareStatement(Parser.replace("SELECT {1} FROM {2}", columns, tableName));
                ResultSet resultSet = select.executeQuery();
                while (resultSet.next()) {
                    ResultSetMetaData rsMetaData = resultSet.getMetaData();
                    StringBuilder tableRowBuffer = new StringBuilder();
                    for (int i = 1; i <= rsMetaData.getColumnCount(); i++) {
                        if (rsMetaData.getColumnType(i) == Types.INTEGER) {
                            tableRowBuffer.append(rsMetaData.getColumnName(i))
                                    .append(": ")
                                    .append(resultSet.getInt(i))
                                    .append(" ");
                        } else {
                            tableRowBuffer.append(rsMetaData.getColumnName(i))
                                    .append(": ")
                                    .append(resultSet.getString(i))
                                    .append(" ");
                        }
                    }
                    tableRowBuffer.delete(tableRowBuffer.length() - 1, tableRowBuffer.length());
                    tableContentList.add(tableRowBuffer.toString());
                }
                select.close();
            } catch (SQLException e) {
                logger.error("Информация из таблицы не получена: " + e.getMessage());
            }
        }
        logger.info("Данные из таблицы получены успешно");

        return tableContentList;
    }

    public static void testJmx(List<java.sql.Connection> connections) {
        try {
            ObjectName objectName = new ObjectName("Main:type=basic,name=tableinfo");
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            server.registerMBean(new TableInfo(connections), objectName);
        } catch (MalformedObjectNameException | InstanceAlreadyExistsException |
                MBeanRegistrationException | NotCompliantMBeanException e) {
            logger.error("Ошибка JMX: " +e.getMessage());
        }
        while (true) {
        }
    }

    public static void replicate(java.sql.Connection syncSourceDB, java.sql.Connection syncTargetDB, Map<String, List<String>> tablesStructure){
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        for (String tableName : tablesStructure.keySet()) {
                executorService.execute(() -> {
                System.out.println(currentThread());
                transferFromSourceToTarget(syncSourceDB, syncTargetDB, tableName, tablesStructure.get(tableName));
                transferFromSourceToTarget(syncTargetDB, syncSourceDB, tableName, tablesStructure.get(tableName));
                });
            }
        executorService.shutdown();
        try {
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        logger.info("Репликация завершена.");
    }

    private static void transferFromSourceToTarget(java.sql.Connection syncSourceDB, java.sql.Connection syncTargetDB, String tableName, List<String> columnList) {
        String columns = Parser.listToString(columnList);
        PreparedStatement select;
        PreparedStatement insert;
        ResultSet resultSet;

        try {
            select = syncTargetDB.prepareStatement(Parser.replace("SELECT {1} FROM {2}", columns, tableName));
            resultSet = select.executeQuery();
            while (resultSet.next()) {
                ResultSetMetaData rsMetaData = resultSet.getMetaData();
                List<String> dataList = new ArrayList<>();
                StringBuilder whereClause = new StringBuilder("WHERE ");
                for (int i = 1; i <= rsMetaData.getColumnCount(); i++) {
                    if (rsMetaData.getColumnType(i) == Types.INTEGER) {
                        dataList.add(String.valueOf(resultSet.getInt(i)));
                    } else {
                        dataList.add(resultSet.getString(i));
                    }
                }
                whereClause.append(IntStream.range(0, columnList.size())
                        .mapToObj(index -> tableName + "." + columnList.get(index) + " = '" + dataList.get(index) + "'")
                        .collect(Collectors.joining(" AND ")));
                insert = syncSourceDB.prepareStatement(Parser.replace("INSERT INTO {1} ({2}) SELECT {3} WHERE NOT EXISTS(SELECT {2} FROM {1} {4})",
                        tableName, columns,
                        dataList.stream().map(string -> "'" + string + "'")
                                .collect(Collectors.joining(", ")),
                        whereClause.toString()));
                insert.execute();
            }
            resultSet.close();
            select.close();
        } catch (SQLException e) {
            logger.error("Ошибка при репликации: " + e.getMessage());
            e.printStackTrace();
        }
    }
}

