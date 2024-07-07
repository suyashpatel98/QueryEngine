package com.dev.logical.plan.expressions;

import com.dev.datatypes.Field;
import com.dev.logical.plan.LogicalExpr;
import com.dev.logical.plan.LogicalPlan;

import java.sql.SQLException;
import java.util.stream.Collectors;

public class Column extends LogicalExpr {
    private final String name;

    public Column(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public Field toField(LogicalPlan input) throws SQLException {
        return input.schema().getFields().stream()
                .filter(field -> field.getName().equals(name))
                .findFirst()
                .orElseThrow(() -> new SQLException("No column named '" + name + "' in " + input.schema().getFields().stream().map(Field::getName).collect(Collectors.toList())));
    }

    @Override
    public String toString() {
        return "#" + name;
    }

    public static Column col(String name) {
        return new Column(name);
    }

    public Eq eq(LiteralString co) {
        return Eq.eq(this, co);
    }

    public Gt gt(LiteralLong lit) {
        return Gt.gt(this, lit);
    }
}
