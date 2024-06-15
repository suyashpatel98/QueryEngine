package com.dev.logical.plan.expressions;

import com.dev.logical.plan.LogicalExpr;

class Gt extends BooleanBinaryExpr {
    Gt(LogicalExpr l, LogicalExpr r) {
        super("gt", ">", l, r);
    }

    public static Gt gt(LogicalExpr lhs, LogicalExpr rhs) {
        return new Gt(lhs, rhs);
    }
}
