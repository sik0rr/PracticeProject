import com.zaxxer.hikari.HikariDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.*;
import java.sql.PreparedStatement;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Main extends Thread {
    private static final Logger logger = LogManager.getLogger(Main.class);


    public static void main(String[] args) throws SQLException {
        if (args.length == 0) {
            logger.error("Отсутствует путь до файла конфигурации");
            return;
            //  throw new IllegalArgumentException("Отсутствует путь до файла конфигурации");
        }
        List<Connection> conList = Parser.yamlParse(args);
        List<HikariDataSource> dsList = Connection.dataSourceList(conList);
        List<java.sql.Connection> javaConList = Connection.javaConList(dsList);
        List<String> columns = new ArrayList<String>() {{ //TODO временное решение пока не прописан парсер
            add("students");
        }};
        Map<String, List<String>> tableStructure = Table.getTableStructure(javaConList.get(0), columns);
        //TODO отрефакторить код
        java.sql.Connection con1 = Connection.getConnection(dsList, 0);
        java.sql.Connection con2 = Connection.getConnection(dsList, 1);
        System.out.println(String.join("\n", getTableContent(con1, tableStructure)));
        replicate(con1, con2, tableStructure);
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
                    tableRowBuffer.delete(tableRowBuffer.length()-1, tableRowBuffer.length());
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

    public static void replicate(java.sql.Connection syncSourceDB, java.sql.Connection syncTargetDB, Map<String, List<String>> tablesStructure) {
        transferFromSourceToTarget(syncSourceDB,syncTargetDB,tablesStructure);
        transferFromSourceToTarget(syncTargetDB,syncSourceDB,tablesStructure);
        try {
            syncSourceDB.close();
            syncTargetDB.close();
        } catch (SQLException e) {
            logger.error("Ошибка при закрытии соединения: " + e.getMessage());
        }
        logger.info("Репликация завершена.");
    }
    private static void transferFromSourceToTarget(java.sql.Connection syncSourceDB, java.sql.Connection syncTargetDB, Map<String, List<String>> tablesStructure) {
        for (String tableName : tablesStructure.keySet()) {
            String columns = Parser.listToString(tablesStructure.get(tableName));
            PreparedStatement select;
            PreparedStatement insert;
            ResultSet resultSet;
            try {
                select = syncTargetDB.prepareStatement(Parser.replace("SELECT {1} FROM {2}", columns, tableName));
                resultSet = select.executeQuery();
            } catch (SQLException e) {
                logger.error("Информация из таблицы не получена: " + e.getMessage());
                continue;
            }
            try {
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
                    whereClause.append(IntStream.range(0, tablesStructure.get(tableName).size())
                            .mapToObj(index -> tableName + "." + tablesStructure.get(tableName).get(index) + " = '" + dataList.get(index) + "'")
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
}

