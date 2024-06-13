package com.dev.logical.plan;

import com.dev.datatypes.Schema;
import java.util.*;

public interface LogicalPlan {
    /**
     * Returns the schema of the data that will be produced by this logical plan.
     */
    Schema schema();

    /**
     * Returns the children (inputs) of this logical plan. This method is used to enable use of the
     * visitor pattern to walk a query tree.
     */
    List<LogicalPlan> children();

    default String pretty() {
        return format(this);
    }

    public static String format(LogicalPlan plan, int indent) {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < indent; i++) {
            b.append("\t");
        }
        b.append(plan.toString()).append("\n");
        for (LogicalPlan child : plan.children()) {
            b.append(format(child, indent + 1));
        }
        return b.toString();
    }

    /**
     * Overloaded method to format a logical plan with default indent of 0.
     */
    public static String format(LogicalPlan plan) {
        return format(plan, 0);
    }
}
