import com.zaxxer.hikari.HikariDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.sql.DriverManager;
import java.util.*;

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
        List<String> studentList1 = studentList(con1, "students");
        List<String> studentList2 = studentList(con2, "students");
        System.out.println("Список студентов из таблицы №1:" + studentList1);
        System.out.println("Список студентов из таблицы №2:" + studentList2);
        String[] tableNames = {"students", "students", "students1", "students2"};
        replicate(con1, con2, "students", tableNames);
    }

    public static List<String> studentList(java.sql.Connection connection, String tableName) {
        List<String> studentList = new ArrayList<>();
        try {
            PreparedStatement select = connection.prepareStatement("SELECT * FROM " + tableName);
            ResultSet resultSet = select.executeQuery();
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

    public static void oneTableToAnother(java.sql.Connection syncSourceDB, java.sql.Connection syncTargetDB, String mainTable, String[] tableNames) {
        try {
            for (String tableName : tableNames) {
                PreparedStatement create = syncTargetDB.prepareStatement("CREATE TABLE IF NOT EXISTS " + tableName + "(id serial primary key, name varchar(30), sex varchar(3), birthday date,year int,specialty varchar(30))");
                create.execute();
                PreparedStatement select = syncTargetDB.prepareStatement("SELECT * FROM " + tableName);
                ResultSet resultSet = select.executeQuery();
                while (resultSet.next()) {
                    String name = resultSet.getString("name");
                    String sex = resultSet.getString("sex");
                    String birthday = resultSet.getDate("birthday").toString();
                    int year = resultSet.getInt("year");
                    String specialty = resultSet.getString("specialty");

                    String insertSql = String.format(Locale.ROOT, "INSERT INTO %s(name,sex,birthday,year,specialty) " +
                                    "SELECT '%s','%s','%s','%d','%s' WHERE NOT EXISTS(SELECT name, sex, birthday, year, specialty " +
                                    "FROM %s st WHERE st.name ='%s' AND st.sex = '%s' AND st.birthday = '%s' AND st.year = '%d' AND st.specialty = '%s')",
                            mainTable, name, sex, birthday, year, specialty, mainTable, name, sex, birthday, year, specialty);
                    PreparedStatement insert = syncSourceDB.prepareStatement(insertSql);
                    insert.execute();
                }
            }
            for (String tableName1 : tableNames) {
                PreparedStatement create = syncTargetDB.prepareStatement("CREATE TABLE IF NOT EXISTS " + tableName1 + "(id serial primary key, name varchar(30), sex varchar(3), birthday date,year int,specialty varchar(30))");
                create.execute();
                PreparedStatement select1 = syncSourceDB.prepareStatement("SELECT * FROM " + mainTable);
                ResultSet resultSet1 = select1.executeQuery();
                while (resultSet1.next()) {
                    String name1 = resultSet1.getString("name");
                    String sex1 = resultSet1.getString("sex");
                    String birthday1 = resultSet1.getDate("birthday").toString();
                    int year1 = resultSet1.getInt("year");
                    String specialty1 = resultSet1.getString("specialty");
                    String reverseInsert = String.format(Locale.ROOT, "INSERT INTO %s(name,sex,birthday,year,specialty) " +
                                    "SELECT '%s','%s','%s','%d','%s' WHERE NOT EXISTS(SELECT name, sex, birthday, year, specialty " +
                                    "FROM %s st WHERE st.name ='%s' AND st.sex = '%s' AND st.birthday = '%s' AND st.year = '%d' AND st.specialty = '%s')",
                            tableName1, name1, sex1, birthday1, year1, specialty1, tableName1, name1, sex1, birthday1, year1, specialty1);
                    PreparedStatement reverse = syncTargetDB.prepareStatement(reverseInsert);
                    reverse.execute();
                }
            }


        } catch (SQLException e) {
            logger.error("Информация из таблицы не получена: " + e.getMessage());
            System.exit(1);
        }
        logger.info("Репликация завершена успешно");
    }

    public static void replicate(java.sql.Connection db1, java.sql.Connection db2, String mainTable, String[]
            tableNames) {
        oneTableToAnother(db1, db2, mainTable, tableNames);
    }
}

