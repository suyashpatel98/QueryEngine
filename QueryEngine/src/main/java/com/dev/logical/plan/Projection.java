package com.dev.logical.plan;

import com.dev.datatypes.Schema;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

public class Projection implements LogicalPlan {
    private final LogicalPlan input;
    private final List<LogicalExpr> expr;

    public Projection(LogicalPlan input, List<LogicalExpr> expr) {
        this.input = input;
        this.expr = expr;
    }

    public LogicalPlan getInput() {
        return input;
    }

    public List<LogicalExpr> getExpr() {
        return expr;
    }

    @Override
    public Schema schema() {
        return new Schema(expr.stream()
                .map(e -> {
                    try {
                        return e.toField(input);
                    } catch (SQLException ex) {
                        throw new RuntimeException(ex);
                    }
                })
                .collect(Collectors.toList()));
    }

    @Override
    public List<LogicalPlan> children() {
        return List.of(input);
    }

    @Override
    public String toString() {
        return "Projection: " + expr.stream()
                .map(LogicalExpr::toString)
                .collect(Collectors.joining(", "));
    }
}
