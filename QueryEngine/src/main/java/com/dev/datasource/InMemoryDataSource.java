package com.dev.datasource;

import com.dev.datatypes.*;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.util.*;
import java.util.stream.*;

import static com.dev.datatypes.ArrowTypes.StringType;


public class InMemoryDataSource implements DataSource {
    private final Schema schema;
    private final List<RecordBatch> data;

    public InMemoryDataSource(Schema schema, List<RecordBatch> data) {
        this.schema = schema;
        this.data = data;
    }

    public static InMemoryDataSource getInMemoryDataSourceInstanceWithDummyData() {
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
        return dataSource;
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Iterable<RecordBatch> scan(List<String> projection) {
        List<Integer> projectionIndices = projection.stream()
                .map(name -> IntStream.range(0, schema.getFields().size())
                        .filter(i -> schema.getFields().get(i).getName().equals(name))
                        .findFirst()
                        .orElseThrow(() -> new IllegalArgumentException("No column named '" + name + "'")))
                .collect(Collectors.toList());

        return data.stream()
                .map(batch -> {
                    List<ColumnVector> projectedFields = projectionIndices.stream()
                            .map(i -> batch.field(i))
                            .collect(Collectors.toList());
                    return new RecordBatch(schema, projectedFields);
                })
                .collect(Collectors.toList());
    }
}
