import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class DB {
    private static final Logger logger = LogManager.getLogger(Main.class);

    private static DatabaseMetaData getMetaData(java.sql.Connection connection) {
        DatabaseMetaData metaData = null;
            try {
                metaData = connection.getMetaData();
            } catch (SQLException e) {
                logger.error("Ошибка при получении мета-данных: " + e.getMessage());
            }
        logger.info("Мета-данные получены");
        return metaData;
    }
    public static Map<String, List<String>> getTableStructure(java.sql.Connection connection, List<String> tableNames) {
        DatabaseMetaData databaseMetaData = getMetaData(connection);
        ResultSet resultSet;
        Map<String, List<String>> tableColumnsMap = new HashMap<>();
        for (String tableName : tableNames) {
            List<String> tableColumnList = new ArrayList<>();
            try {
                resultSet = databaseMetaData.getColumns(null,null,tableName,null);
                while (resultSet.next()) {
                    if (resultSet.getString("COLUMN_NAME").equals("id")) {
                        continue;
                    }
                    tableColumnList.add(resultSet.getString("COLUMN_NAME"));
                }
                tableColumnsMap.put(tableName,tableColumnList);
            } catch (SQLException e) {
                logger.error("Ошибка получения структуры таблицы:" +e.getMessage());
            }
        }
        return tableColumnsMap;
    }
}
