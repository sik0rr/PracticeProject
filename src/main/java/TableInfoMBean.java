import org.slf4j.Logger;

import java.util.List;
import java.util.Map;

public interface TableInfoMBean {
     List<String> getTableContent(String table, int conNumber);
}
