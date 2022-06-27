import com.zaxxer.hikari.HikariDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class Main {
    private static final Logger logger = LogManager.getLogger(Main.class);

    public static void main(String[] args) throws SQLException {
        if (args.length == 0) {
            logger.error("Отсутствует путь до файла конфигурации");
            return;
        }
        List<Connection> conList = Connection.yamlParse(args);
        HikariDataSource dataSource1 = Connection.dsInit(conList.get(0));
        HikariDataSource dataSource2 = Connection.dsInit(conList.get(1));
        java.sql.Connection con1 = Connection.getConnection(dataSource1);
        java.sql.Connection con2 = Connection.getConnection(dataSource2);
        List<String> studentList1 = studentList(con1);
        List<String> studentList2 = studentList(con2);
        System.out.println("Список студентов из таблицы №1:" + studentList1);
        System.out.println("Список студентов из таблицы №2:" + studentList2);
    }
    public static List<String> studentList (java.sql.Connection connection) {
        List<String> studentList = new ArrayList<>();
        try {
            PreparedStatement statement = connection.prepareStatement("SELECT * FROM students");
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                int id = resultSet.getInt("id");
                String name = resultSet.getString("name");
                String sex = resultSet.getString("sex");
                String birthday = resultSet.getDate("birthday").toString();
                int year = resultSet.getInt("year");
                String specialty = resultSet.getString("specialty");
                String result = "\nИмя: " + name + ", Пол: " + sex + ", Дата рождения: " + birthday + ", Курс: " + year + ", Специальность: " + specialty;
                studentList.add(result);
            }
        } catch (SQLException e) {
            logger.error("Информация из таблицы не получена: " + e.getMessage());
            System.exit(1);
        }
        logger.info("Данные из таблицы получены успешно");
        return studentList;
    }
}
