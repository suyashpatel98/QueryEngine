package com.dev.physical.plan;

import com.dev.datatypes.*;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

public class SelectionExec implements PhysicalPlan {
    private final PhysicalPlan input;
    private final Expression expr;

    public SelectionExec(PhysicalPlan input, Expression expr) {
        this.input = input;
        this.expr = expr;
    }

    @Override
    public Schema getSchema() {
        return input.getSchema();
    }

    @Override
    public List<PhysicalPlan> getChildren() {
        return List.of(input);
    }

    @Override
    public Iterable<RecordBatch> execute() throws FileNotFoundException {
        Iterable<RecordBatch> inputBatches = input.execute();
        return () -> StreamSupport.stream(inputBatches.spliterator(), false)
                .map(batch -> {
                    BitVector result = (BitVector) ((ArrowFieldVector) expr.evaluate(batch)).getField();
                    Schema schema = batch.getSchema();
                    int columnCount = batch.getSchema().getFields().size();
                    List<FieldVector> filteredFields = IntStream.range(0, columnCount)
                            .mapToObj(i -> filter(batch.getField(i), result))
                            .collect(Collectors.toList());
                    List<ColumnVector> fields = filteredFields.stream()
                            .map(ArrowFieldVector::new)
                            .collect(Collectors.toList());
                    return new RecordBatch(schema, fields);
                })
                .iterator();
    }

    private FieldVector filter(ColumnVector v, BitVector selection) {
        VarCharVector filteredVector = new VarCharVector("v", new RootAllocator(Long.MAX_VALUE));
        filteredVector.allocateNew();

        ArrowVectorBuilder builder = new ArrowVectorBuilder(filteredVector);

        int count = 0;
        for (int i = 0; i < selection.getValueCount(); i++) {
            if (selection.get(i) == 1) {
                builder.set(count, v.getValue(i));
                count++;
            }
        }
        filteredVector.setValueCount(count);
        return filteredVector;
    }

    @Override
    public String toString() {
        return "SelectionExec: " + expr;
    }
}
