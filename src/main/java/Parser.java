import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Parser {
    private static final Logger logger = LogManager.getLogger(Connection.class);

    public static List<Connection> yamlParse(String[] paths) {
        Yaml yalm = new Yaml(new Constructor(Connection[].class));
        List<Connection> connectionsList = new LinkedList<>();
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

    public static String listToString(List<String> stringList) {
        return stringList
                .stream()
                .map(String::valueOf)
                .collect(Collectors.joining(", "));
    }

    public static String replace(String pattern, String... args) {
        for (int i = 0; i < args.length; i++) {
            pattern = pattern.replace("{" + (i + 1) + "}", args[i]);
        }
        return pattern;
    }
}
