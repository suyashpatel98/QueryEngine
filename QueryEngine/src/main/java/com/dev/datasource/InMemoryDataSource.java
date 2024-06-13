package com.dev.datasource;

import com.dev.datatypes.ColumnVector;
import com.dev.datatypes.RecordBatch;
import com.dev.datatypes.Schema;
import java.util.*;
import java.util.stream.*;


public class InMemoryDataSource implements DataSource {
    private final Schema schema;
    private final List<RecordBatch> data;

    public InMemoryDataSource(Schema schema, List<RecordBatch> data) {
        this.schema = schema;
        this.data = data;
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
