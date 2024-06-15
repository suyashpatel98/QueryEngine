package com.dev.logical.plan.expressions;

import com.dev.datatypes.ArrowTypes;
import com.dev.datatypes.Field;
import com.dev.logical.plan.LogicalExpr;
import com.dev.logical.plan.LogicalPlan;

public class LiteralString extends LogicalExpr {
    private final String str;

    public LiteralString(String str) {
        this.str = str;
    }

    @Override
    public Field toField(LogicalPlan input) {
        return new Field(str, ArrowTypes.StringType);
    }

    @Override
    public String toString() {
        return "'" + str + "'";
    }

    public static LiteralString litString(String value) {
        return new LiteralString(value);
    }

    public static LiteralLong litLong(Long value) {
        return new LiteralLong(value);
    }
}