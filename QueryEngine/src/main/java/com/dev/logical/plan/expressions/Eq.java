package com.dev.logical.plan.expressions;

import com.dev.logical.plan.LogicalExpr;

public class Eq extends BooleanBinaryExpr {
    public Eq(LogicalExpr l, LogicalExpr r) {
        super("eq", "=", l, r);
    }
}
