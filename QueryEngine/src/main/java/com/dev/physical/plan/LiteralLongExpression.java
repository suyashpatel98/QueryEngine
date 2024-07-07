package com.dev.physical.plan;

import com.dev.datatypes.ArrowTypes;
import com.dev.datatypes.ColumnVector;
import com.dev.datatypes.RecordBatch;

public class LiteralLongExpression implements Expression {
    private final long value;

    public LiteralLongExpression(long value) {
        this.value = value;
    }

    @Override
    public ColumnVector evaluate(RecordBatch input) {
        return new LiteralValueVector(ArrowTypes.Int64Type, value, input.rowCount());
    }
}
