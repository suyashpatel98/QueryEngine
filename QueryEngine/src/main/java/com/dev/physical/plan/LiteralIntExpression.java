package com.dev.physical.plan;

import com.dev.datatypes.ArrowTypes;
import com.dev.datatypes.ColumnVector;
import com.dev.datatypes.RecordBatch;

public class LiteralIntExpression implements Expression {
    private final int value;

    public LiteralIntExpression(int value) {
        this.value = value;
    }

    @Override
    public ColumnVector evaluate(RecordBatch input) {
        return new LiteralValueVector(ArrowTypes.Int32Type, value, input.rowCount());
    }
}
