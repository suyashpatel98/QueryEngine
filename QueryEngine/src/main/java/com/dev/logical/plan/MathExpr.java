package com.dev.logical.plan;

import com.dev.datatypes.Field;
import com.dev.logical.plan.expressions.BinaryExpr;

import java.sql.SQLException;

abstract class MathExpr extends BinaryExpr {
    MathExpr(String name, String op, LogicalExpr l, LogicalExpr r) {
        super(name, op, l, r);
    }

    @Override
    public Field toField(LogicalPlan input) throws SQLException {
        return new Field(this.name, this.l.toField(input).getDataType());
    }
}

class Add extends MathExpr {
    Add(LogicalExpr l, LogicalExpr r) {
        super("add", "+", l, r);
    }
}

class Subtract extends MathExpr {
    Subtract(LogicalExpr l, LogicalExpr r) {
        super("subtract", "-", l, r);
    }
}

class Multiply extends MathExpr {
    Multiply(LogicalExpr l, LogicalExpr r) {
        super("mult", "*", l, r);
    }
}

class Divide extends MathExpr {
    Divide(LogicalExpr l, LogicalExpr r) {
        super("div", "/", l, r);
    }
}

class Modulus extends MathExpr {
    Modulus(LogicalExpr l, LogicalExpr r) {
        super("mod", "%", l, r);
    }
}
