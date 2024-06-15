package com.dev.logical.plan;

import com.dev.datatypes.Schema;

import java.util.List;

public interface DataFrame {

    /**
     * Apply a projection.
     *
     * @param expr The list of logical expressions to project.
     * @return A new DataFrame with the projection applied.
     */
    DataFrame project(List<LogicalExpr> expr);

    /**
     * Apply a filter.
     *
     * @param expr The logical expression to filter by.
     * @return A new DataFrame with the filter applied.
     */
    DataFrame filter(LogicalExpr expr);

    /**
     * Returns the schema of the data that will be produced by this DataFrame.
     *
     * @return The schema of the data.
     */
    Schema schema();

    /**
     * Get the logical plan associated with this DataFrame.
     *
     * @return The logical plan.
     */
    LogicalPlan logicalPlan();
}
