package com.dev.logical.plan;

import com.dev.datatypes.Schema;

import java.util.*;

public class DataFrameImpl implements DataFrame {
    private final LogicalPlan plan;

    public DataFrameImpl(LogicalPlan plan) {
        this.plan = plan;
    }

    @Override
    public DataFrame project(List<LogicalExpr> expr) {
        return new DataFrameImpl(new Projection(plan, expr));
    }

    @Override
    public DataFrame filter(LogicalExpr expr) {
        return new DataFrameImpl(new Selection(plan, expr));
    }

    @Override
    public Schema schema() {
        return plan.schema();
    }

    @Override
    public LogicalPlan logicalPlan() {
        return plan;
    }
}
