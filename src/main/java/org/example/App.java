package org.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class App {
    static String url = "jdbc:postgresql://localhost:5432/postgres";
    static String username = "postgres";
    static String password = "43760097";
    static int countDb = 0;

    public static void main(String[] args) {
        // Создаем и запускаем три бд
        for(int i = 1; i <= 3; i++){
            countDb++;
            createDatabase("db_" + countDb);
            createTable("db_" + countDb);
        }
        System.out.println();

        // Создаем потоками 5 бд
        Thread thread = null;
        for (int i = 0; i < 5; i++) {
            final int finalI = ++countDb;
            thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    createDatabaseFromTemplate("db_" + finalI, "db_1");
                }
            });
            thread.start();
        }
        try {
            thread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }


        for (int i = 0; i < 100; i++) {
            String randomDbName = "db_" + ((int) (Math.random() * countDb) + 1);
            int randomValue = (int) (Math.random() * 1000) + 1;
            queryRandomValue(randomDbName, randomValue);
        }
    }

    private static void queryRandomValue(String dbName, int value) {
        try (Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/" + dbName, username, password)) {
            String querySQL = "SELECT column_name FROM table_name WHERE column_name = " + value;
            try (Statement statement = connection.createStatement()) {
                if (statement.executeQuery(querySQL).next()) {
                    System.out.println("Значение " + value + " найдено в базе данных " + dbName);
                } else {
                    System.out.println("Значение " + value + " не найдено в базе данных " + dbName);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void createDatabase(String dbName) {
        try (Connection connection = DriverManager.getConnection(url, username, password)) {
            String createDbSQL = "CREATE DATABASE " + dbName;
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate(createDbSQL);
                System.out.println("БД " + dbName + " создана успешно!");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void createDatabaseFromTemplate(String dbName, String templateName) {
        System.out.println("БД " + dbName + " создается...");
        try (Connection connection = DriverManager.getConnection(url, username, password)) {
            String createDbFromTemplateSQL = "CREATE DATABASE " + dbName + " TEMPLATE " + templateName;
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate(createDbFromTemplateSQL);
                System.out.println("БД " + dbName + " создана по шаблону успешно!");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void createTable(String dbName) {
        try (Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/" + dbName, username, password)) {
            String createTableSQL = "CREATE TABLE table_name (column_name INT PRIMARY KEY)";
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate(createTableSQL);
                System.out.println("Таблица " + dbName + " создана в бд успешно!");

                for (int i = 1; i <= 1000; i++) {
                    statement.executeUpdate("INSERT INTO table_name (column_name) VALUES (?)".replace("?", String.valueOf(i)));
                }
                System.out.println("Значения, вставленны в таблицу БД  " + dbName + " успешно!");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
