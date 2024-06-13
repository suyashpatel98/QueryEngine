package com.dev.logical.plan.expressions;

import com.dev.datatypes.ArrowTypes;
import com.dev.datatypes.Field;
import com.dev.logical.plan.LogicalExpr;
import com.dev.logical.plan.LogicalPlan;

abstract class BooleanBinaryExpr extends BinaryExpr {
    BooleanBinaryExpr(String name, String op, LogicalExpr l, LogicalExpr r) {
        super(name, op, l, r);
    }

    @Override
    public Field toField(LogicalPlan input) {
        return new Field(this.name, ArrowTypes.BooleanType);
    }
}
