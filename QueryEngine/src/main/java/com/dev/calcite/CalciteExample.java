package com.dev.calcite;

import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.*;

import java.sql.*;
import java.util.Properties;

public class CalciteExample {

    public static class Employee {
        public final String name;
        public final int age;
        public final String department;

        public Employee(String name, int age, String department) {
            this.name = name;
            this.age = age;
            this.department = department;
        }
    }

    public static class EmployeeSchema {
        public final Employee[] EMPLOYEES = {
                new Employee("John Doe", 30, "IT"),
                new Employee("Jane Smith", 25, "HR"),
                new Employee("Bob Johnson", 35, "Finance"),
                new Employee("Alice Brown", 28, "Marketing")
        };
    }

    public static void main(String[] args) {
        try {
            Class.forName("org.apache.calcite.jdbc.Driver");

            Properties info = new Properties();
            info.setProperty("lex", "JAVA");
            Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
            CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

            SchemaPlus rootSchema = calciteConnection.getRootSchema();
            rootSchema.add("COMPANY", new ReflectiveSchema(new EmployeeSchema()));

            String sql = "SELECT * FROM COMPANY.EMPLOYEES";

            Statement statement = calciteConnection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);

            System.out.println("Query Results:");
            while (resultSet.next()) {
                String name = resultSet.getString("name");
                int age = resultSet.getInt("age");
                String department = resultSet.getString("department");
                System.out.printf("Name: %s, Age: %d, Department: %s%n", name, age, department);
            }

            // Optionally, you can also demonstrate the query plan
            FrameworkConfig config = Frameworks.newConfigBuilder()
                    .defaultSchema(rootSchema)
                    .build();
            Planner planner = Frameworks.getPlanner(config);

            SqlNode sqlNode = planner.parse(sql);
            SqlNode validatedSqlNode = planner.validate(sqlNode);
            RelRoot relRoot = planner.rel(validatedSqlNode);

            System.out.println("\nQuery Plan:");
            System.out.println(RelOptUtil.toString(relRoot.project()));

            resultSet.close();
            statement.close();
            connection.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
