package com.dev.physical.plan;

import com.dev.datatypes.ArrowTypes;
import org.apache.arrow.vector.types.pojo.ArrowType;

public class EqExpression extends BooleanExpression {

    public EqExpression(Expression l, Expression r) {
        super(l, r);
    }

    @Override
    protected boolean evaluate(Object l, Object r, ArrowType arrowType) {
        if (arrowType.equals(ArrowTypes.Int8Type)) {
            return ((Byte) l).equals((Byte) r);
        } else if (arrowType.equals(ArrowTypes.Int16Type)) {
            return ((Short) l).equals((Short) r);
        } else if (arrowType.equals(ArrowTypes.Int32Type)) {
            return ((Integer) l).equals((Integer) r);
        } else if (arrowType.equals(ArrowTypes.Int64Type)) {
            return ((Long) l).equals((Long) r);
        } else if (arrowType.equals(ArrowTypes.FloatType)) {
            return ((Float) l).equals((Float) r);
        } else if (arrowType.equals(ArrowTypes.DoubleType)) {
            return ((Double) l).equals((Double) r);
        } else if (arrowType.equals(ArrowTypes.StringType)) {
            return toString(l).equals(toString(r));
        } else {
            throw new IllegalStateException(
                    "Unsupported data type in comparison expression: " + arrowType);
        }
    }

    private String toString(Object obj) {
        return obj == null ? null : obj.toString();
    }
}
