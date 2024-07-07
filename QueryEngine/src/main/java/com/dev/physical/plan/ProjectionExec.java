package com.dev.physical.plan;

import com.dev.datatypes.ColumnVector;
import com.dev.datatypes.RecordBatch;
import com.dev.datatypes.Schema;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ProjectionExec implements PhysicalPlan {
    private final PhysicalPlan input;
    private final Schema schema;
    private final List<Expression> expr;

    public ProjectionExec(PhysicalPlan input, Schema schema, List<Expression> expr) {
        this.input = input;
        this.schema = schema;
        this.expr = expr;
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    @Override
    public List<PhysicalPlan> getChildren() {
        return List.of(input);
    }

    @Override
    public Iterable<RecordBatch> execute() throws FileNotFoundException {
        return () -> {
            try {
                return StreamSupport.stream(input.execute().spliterator(), false)
                        .map(batch -> {
                            List<ColumnVector> columns = expr.stream()
                                    .map(expression -> expression.evaluate(batch))
                                    .collect(Collectors.toList());
                            return new RecordBatch(schema, columns);
                        })
                        .iterator();
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
        };
    }

    @Override
    public String toString() {
        return "ProjectionExec: " + expr;
    }
}
