package com.dev.logical.plan.expressions;

import com.dev.logical.plan.LogicalExpr;

public class Gt extends BooleanBinaryExpr {
    public Gt(LogicalExpr l, LogicalExpr r) {
        super("gt", ">", l, r);
    }

    public static Gt gt(LogicalExpr lhs, LogicalExpr rhs) {
        return new Gt(lhs, rhs);
    }
}
