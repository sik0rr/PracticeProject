import org.slf4j.Logger;

import java.sql.*;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TableInfo implements TableInfoMBean {
    List<java.sql.Connection> connections;
    TableInfo(List<java.sql.Connection> connections){
        this.connections = connections;
    }
    @Override
    public List<String> getTableContent(String table, int conNumber) {
        List<String> tableContentList = new ArrayList<>();
        PreparedStatement select;
        if (conNumber>connections.size()-1) {
            throw new IllegalArgumentException("Номер соединения указан неверно");
        }
            try {
                select = connections.get(conNumber).prepareStatement(Parser.replace("SELECT * FROM {1}", table));
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
            }

        return tableContentList;
    }

}
