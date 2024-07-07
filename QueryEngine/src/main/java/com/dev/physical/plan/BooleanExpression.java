package com.dev.physical.plan;

import com.dev.datatypes.ArrowFieldVector;
import com.dev.datatypes.ColumnVector;
import com.dev.datatypes.RecordBatch;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.types.pojo.ArrowType;

public abstract class BooleanExpression implements Expression {
    protected final Expression l;
    protected final Expression r;

    public BooleanExpression(Expression l, Expression r) {
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
                    "Cannot compare values of different type: " + ll.getType() + " != " + rr.getType());
        }
        return compare(ll, rr);
    }

    protected ColumnVector compare(ColumnVector l, ColumnVector r) {
        BitVector v = new BitVector("v", new RootAllocator(Long.MAX_VALUE));
        v.allocateNew();
        for (int i = 0; i < l.size(); i++) {
            boolean value = evaluate(l.getValue(i), r.getValue(i), l.getType());
            v.set(i, value ? 1 : 0);
        }
        v.setValueCount(l.size());
        return new ArrowFieldVector(v);
    }

    protected abstract boolean evaluate(Object l, Object r, ArrowType arrowType);
}
