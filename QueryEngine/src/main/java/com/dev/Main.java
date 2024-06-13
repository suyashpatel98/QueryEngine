package com.dev;

import com.dev.datasource.InMemoryDataSource;
import com.dev.datatypes.*;
import com.dev.logical.plan.LogicalExpr;
import com.dev.logical.plan.Projection;
import com.dev.logical.plan.Scan;
import com.dev.logical.plan.Selection;
import com.dev.logical.plan.expressions.Column;
import com.dev.logical.plan.expressions.Eq;
import com.dev.logical.plan.expressions.LiteralString;
import com.sun.jdi.DoubleType;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.beans.Expression;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.dev.datatypes.ArrowTypes.StringType;
import static com.dev.logical.plan.LogicalPlan.format;

public class Main {
    public static void main(String[] args) {
        List<String> columnNames = List.of("id", "first_name", "last_name", "state", "job_title", "salary");

        List<ArrowType> columnTypes = List.of(
                ArrowTypes.Int32Type, // id
                StringType, // first_name
                StringType, // last_name
                StringType, // state
                StringType, // job_title
                ArrowTypes.Int32Type // salary
        );

        List<Field> fields = IntStream.range(0, columnNames.size())
                .mapToObj(i -> new Field(columnNames.get(i), columnTypes.get(i)))
                .collect(Collectors.toList());
        Schema schema = new Schema(fields);

        List<ColumnVector> columnVectors1 = new ArrayList<>();
        for (Field field : fields) {
            ArrowType type = field.getDataType();
            FieldVector vector = FieldVectorFactory.create(type, 5); // Initial capacity of 5
            ColumnVector columnVector = new ArrowFieldVector(vector);

            // Populate the vector with data
            if (type.equals(ArrowTypes.Int32Type)) {
                IntVector intVector = (IntVector) vector;
                intVector.setSafe(0, 1);
                intVector.setSafe(1, 2);
                intVector.setSafe(2, 3);
                intVector.setSafe(3, 4);
                intVector.setSafe(4, 5);
                intVector.setValueCount(5);
            } else if (type.equals(ArrowTypes.StringType)) {
                VarCharVector varCharVector = (VarCharVector) vector;
                varCharVector.setSafe(0, "John".getBytes());
                varCharVector.setSafe(1, "Jane".getBytes());
                varCharVector.setSafe(2, "Bob".getBytes());
                varCharVector.setSafe(3, "Alice".getBytes());
                varCharVector.setSafe(4, "Charlie".getBytes());
                varCharVector.setValueCount(5);
            }

            columnVectors1.add(columnVector);
        }
        RecordBatch batch1 = new RecordBatch(schema, columnVectors1);

        // Create some sample data
        List<RecordBatch> data = List.of(batch1);

        InMemoryDataSource dataSource = new InMemoryDataSource(schema, data);

        Scan scan = new Scan("employee", dataSource, Collections.emptyList());

        // create a plan to represent the selection (WHERE)
        LogicalExpr filterExpr = new Eq(new Column("state"), new LiteralString("CO"));
        Selection selection = new Selection(scan, filterExpr);

        // create a plan to represent the projection (SELECT)
        List<LogicalExpr> projectionList = Arrays.asList(
                new Column("id"),
                new Column("first_name"),
                new Column("last_name"),
                new Column("state"),
                new Column("salary")
        );
        Projection plan = new Projection(selection, projectionList);

        // print the plan
        System.out.println(format(plan));
    }
}
