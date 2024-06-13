package com.dev.logical.plan.expressions;

import com.dev.datatypes.ArrowTypes;
import com.dev.datatypes.Field;
import com.dev.logical.plan.LogicalExpr;
import com.dev.logical.plan.LogicalPlan;

class LiteralLong extends LogicalExpr {
    private final long n;

    LiteralLong(long n) {
        this.n = n;
    }

    @Override
    public Field toField(LogicalPlan input) {
        return new Field(Long.toString(n), ArrowTypes.Int64Type);
    }

    @Override
    public String toString() {
        return Long.toString(n);
    }
}
