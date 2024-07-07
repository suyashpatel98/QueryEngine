package com.dev.logical.plan;

import com.dev.datatypes.Schema;

import java.util.List;

public class Selection implements LogicalPlan {
    private final LogicalPlan input;
    private final LogicalExpr expr;

    public LogicalPlan getInput() {
        return input;
    }

    public LogicalExpr getExpr() {
        return expr;
    }

    public Selection(LogicalPlan input, LogicalExpr expr) {
        this.input = input;
        this.expr = expr;
    }

    @Override
    public Schema schema() {
        return input.schema();
    }

    @Override
    public List<LogicalPlan> children() {
        // selection does not change the schema of the input
        return List.of(input);
    }

    @Override
    public String toString() {
        return "Selection: " + expr;
    }
}
