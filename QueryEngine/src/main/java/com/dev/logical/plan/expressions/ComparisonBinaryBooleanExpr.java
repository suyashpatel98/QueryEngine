package com.dev.logical.plan.expressions;

import com.dev.logical.plan.LogicalExpr;







class Neq extends BooleanBinaryExpr {
    Neq(LogicalExpr l, LogicalExpr r) {
        super("neq", "!=", l, r);
    }
}



class GtEq extends BooleanBinaryExpr {
    GtEq(LogicalExpr l, LogicalExpr r) {
        super("gteq", ">=", l, r);
    }
}

class Lt extends BooleanBinaryExpr {
    Lt(LogicalExpr l, LogicalExpr r) {
        super("lt", "<", l, r);
    }
}

class LtEq extends BooleanBinaryExpr {
    LtEq(LogicalExpr l, LogicalExpr r) {
        super("lteq", "<=", l, r);
    }
}
