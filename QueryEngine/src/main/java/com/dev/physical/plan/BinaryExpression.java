package com.dev.physical.plan;

import com.dev.datatypes.ColumnVector;
import com.dev.datatypes.RecordBatch;

public abstract class BinaryExpression implements Expression {
    protected final Expression l;
    protected final Expression r;

    public BinaryExpression(Expression l, Expression r) {
        this.l = l;
        this.r = r;
    }

    @Override
    public ColumnVector evaluate(RecordBatch input) {
        ColumnVector ll = l.evaluate(input);
        ColumnVector rr = r.evaluate(input);
        assert ll.size() == rr.size();
        if (!ll.getType().equals(rr.getType())) {
            throw new IllegalStateException(
                    "Binary expression operands do not have the same type: " +
                            ll.getType() + " != " + rr.getType());
        }
        return evaluate(ll, rr);
    }

    protected abstract ColumnVector evaluate(ColumnVector l, ColumnVector r);
}
