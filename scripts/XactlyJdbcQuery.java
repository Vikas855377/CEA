import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Base64;

public class XactlyJdbcQuery {
    public static void main(String[] args) throws Exception {
        if (args.length == 1 && "--server".equals(args[0])) {
            runServer();
            return;
        }

        if (args.length < 2) {
            throw new IllegalArgumentException("Usage: XactlyJdbcQuery <maxRows> <sql>");
        }

        String driverClass = requireEnv("XACTLY_JDBC_DRIVER_CLASS");
        String jdbcUrl = normalizeJdbcUrl(requireEnv("XACTLY_JDBC_URL"));
        String username = requireEnv("XACTLY_JDBC_USER");
        String password = requireEnv("XACTLY_JDBC_PASSWORD");
        int maxRows = Integer.parseInt(args[0]);
        String sql = args[1];

        Class.forName(driverClass);
        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
             Statement statement = connection.createStatement()) {
            System.out.println(executeQuery(statement, sql, maxRows));
        }
    }

    private static void runServer() throws Exception {
        String driverClass = requireEnv("XACTLY_JDBC_DRIVER_CLASS");
        String jdbcUrl = normalizeJdbcUrl(requireEnv("XACTLY_JDBC_URL"));
        String username = requireEnv("XACTLY_JDBC_USER");
        String password = requireEnv("XACTLY_JDBC_PASSWORD");

        Class.forName(driverClass);
        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
             BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
            System.out.println("{\"ready\":true}");
            System.out.flush();

            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t", 3);
                if (parts.length != 3) {
                    System.out.println("{\"request_id\":\"\",\"ok\":false,\"error\":\"Invalid request\"}");
                    System.out.flush();
                    continue;
                }

                String requestId = parts[0];
                int maxRows = Integer.parseInt(parts[1]);
                String sql = new String(Base64.getDecoder().decode(parts[2]));

                try (Statement statement = connection.createStatement()) {
                    String result = executeQuery(statement, sql, maxRows);
                    StringBuilder output = new StringBuilder();
                    output.append("{\"request_id\":");
                    appendJsonString(output, requestId);
                    output.append(",\"ok\":true,\"result\":").append(result).append("}");
                    System.out.println(output);
                } catch (Exception exception) {
                    StringBuilder output = new StringBuilder();
                    output.append("{\"request_id\":");
                    appendJsonString(output, requestId);
                    output.append(",\"ok\":false,\"error\":");
                    appendJsonString(output, exception.toString());
                    output.append("}");
                    System.out.println(output);
                }
                System.out.flush();
            }
        }
    }

    private static String executeQuery(Statement statement, String sql, int maxRows) throws Exception {
        statement.setMaxRows(maxRows);
        boolean hasResultSet = statement.execute(sql);

        if (!hasResultSet) {
            return "{\"columns\":[],\"rows\":[],\"row_count\":" + statement.getUpdateCount() + "}";
        }

        try (ResultSet resultSet = statement.getResultSet()) {
            ResultSetMetaData metadata = resultSet.getMetaData();
            int columnCount = metadata.getColumnCount();

            StringBuilder output = new StringBuilder();
            output.append("{\"columns\":[");
            for (int column = 1; column <= columnCount; column++) {
                if (column > 1) {
                    output.append(",");
                }
                appendJsonString(output, metadata.getColumnLabel(column));
            }
            output.append("],\"rows\":[");

            int rowCount = 0;
            while (resultSet.next() && rowCount < maxRows) {
                if (rowCount > 0) {
                    output.append(",");
                }
                output.append("[");
                for (int column = 1; column <= columnCount; column++) {
                    if (column > 1) {
                        output.append(",");
                    }
                    String value = resultSet.getString(column);
                    if (resultSet.wasNull()) {
                        output.append("null");
                    } else {
                        appendJsonString(output, value);
                    }
                }
                output.append("]");
                rowCount++;
            }

            output.append("],\"row_count\":").append(rowCount).append("}");
            return output.toString();
        }
    }

    private static String requireEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Missing environment variable: " + name);
        }
        return value.trim();
    }

    private static String normalizeJdbcUrl(String url) {
        if (url.startsWith("xactly://")) {
            return "jdbc:" + url;
        }
        return url;
    }

    private static void appendJsonValue(StringBuilder output, Object value) {
        if (value == null) {
            output.append("null");
            return;
        }
        if (value instanceof Number || value instanceof Boolean) {
            output.append(value);
            return;
        }
        appendJsonString(output, value.toString());
    }

    private static void appendJsonString(StringBuilder output, String value) {
        output.append("\"");
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            switch (character) {
                case '"':
                    output.append("\\\"");
                    break;
                case '\\':
                    output.append("\\\\");
                    break;
                case '\b':
                    output.append("\\b");
                    break;
                case '\f':
                    output.append("\\f");
                    break;
                case '\n':
                    output.append("\\n");
                    break;
                case '\r':
                    output.append("\\r");
                    break;
                case '\t':
                    output.append("\\t");
                    break;
                default:
                    if (character < 0x20) {
                        output.append(String.format("\\u%04x", (int) character));
                    } else {
                        output.append(character);
                    }
            }
        }
        output.append("\"");
    }
}
