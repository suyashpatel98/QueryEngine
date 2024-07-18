package com.dev.logical.plan.expressions;

import com.dev.datatypes.ArrowTypes;
import com.dev.datatypes.Field;
import com.dev.logical.plan.LogicalExpr;
import com.dev.logical.plan.LogicalPlan;

public class LiteralInt extends LogicalExpr {
    private final int n;

    public LiteralInt(int n) {
        this.n = n;
    }

    @Override
    public Field toField(LogicalPlan input) {
        return new Field(Long.toString(n), ArrowTypes.Int32Type);
    }

    @Override
    public String toString() {
        return Long.toString(n);
    }

    public int getN() {
        return n;
    }

    public static LiteralInt lit(int value) {
        return new LiteralInt(value);
    }
}
