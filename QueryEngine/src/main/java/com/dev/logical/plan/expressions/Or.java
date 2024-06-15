package com.dev.logical.plan.expressions;

import com.dev.logical.plan.LogicalExpr;

class Or extends BooleanBinaryExpr {
    Or(LogicalExpr l, LogicalExpr r) {
        super("or", "OR", l, r);
    }
}
