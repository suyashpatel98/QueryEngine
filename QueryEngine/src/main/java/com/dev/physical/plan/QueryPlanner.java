package com.dev.physical.plan;

import com.dev.datatypes.Field;
import com.dev.datatypes.Schema;
import com.dev.logical.plan.*;
import com.dev.logical.plan.expressions.*;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

public class QueryPlanner {

    public static PhysicalPlan createPhysicalPlan(LogicalPlan plan) throws SQLException {
        if (plan instanceof Scan) {
            Scan scan = (Scan) plan;
            return new ScanExec(scan.getDataSource(), scan.getProjection());
        } else if (plan instanceof Selection) {
            Selection selection = (Selection) plan;
            PhysicalPlan input = createPhysicalPlan(selection.getInput());
            Expression filterExpr = createPhysicalExpr(selection.getExpr(), selection.getInput());
            return new SelectionExec(input, filterExpr);
        } else if (plan instanceof Projection) {
            Projection projection = (Projection) plan;
            PhysicalPlan input = createPhysicalPlan(projection.getInput());
            List<Expression> projectionExpr = projection.getExpr().stream()
                    .map(expr -> {
                        try {
                            return createPhysicalExpr(expr, projection.getInput());
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .collect(Collectors.toList());
            Schema projectionSchema = new Schema(projection.getExpr().stream()
                    .map(expr -> {
                        try {
                            return expr.toField(projection.getInput());
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .collect(Collectors.toList()));
            return new ProjectionExec(input, projectionSchema, projectionExpr);
        } else {
            throw new IllegalStateException(plan.getClass().toString());
        }
    }

    public static Expression createPhysicalExpr(LogicalExpr expr, LogicalPlan input) throws SQLException {
        if (expr instanceof LiteralLong) {
            return new LiteralLongExpression(((LiteralLong) expr).getN());
        } else if (expr instanceof Column) {
            String columnName = ((Column) expr).getName();
            int i = findColumnIndex(input.schema().getFields(), columnName);
            if (i == -1) {
                throw new SQLException("No column named '" + columnName + "'");
            }
            return new ColumnExpression(i);
        } else if (expr instanceof BinaryExpr) {
            BinaryExpr binaryExpr = (BinaryExpr) expr;
            Expression l = createPhysicalExpr(binaryExpr.getL(), input);
            Expression r = createPhysicalExpr(binaryExpr.getR(), input);

            if (expr instanceof Eq) return new EqExpression(l, r);
            else throw new IllegalStateException("Unsupported binary expression: " + expr);
        }

        throw new IllegalStateException("Unsupported logical expression: " + expr);
    }

    private static int findColumnIndex(List<Field> fields, String columnName) {
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).getName().equals(columnName)) {
                return i;
            }
        }
        return -1;
    }
}
