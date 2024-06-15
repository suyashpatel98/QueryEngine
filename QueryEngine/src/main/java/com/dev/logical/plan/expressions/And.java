package com.dev.logical.plan.expressions;

import com.dev.logical.plan.LogicalExpr;

class And extends BooleanBinaryExpr {
    And(LogicalExpr l, LogicalExpr r) {
        super("and", "AND", l, r);
    }
}
