package com.dev.calcite;

import com.dev.logical.plan.LogicalPlan;
import com.dev.logical.plan.*;

import java.math.BigDecimal;
import java.util.*;

import com.dev.logical.plan.expressions.Column;
import com.dev.logical.plan.expressions.Eq;
import com.dev.logical.plan.expressions.LiteralInt;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Collections;
import java.util.stream.Collectors;

public class MyRelImplementor {
    public LogicalPlan implement(RelNode relNode) {
        if (relNode instanceof MyScanRel) {
            MyScanRel scanRel = (MyScanRel) relNode;
            MyTable myTable = (MyTable) scanRel.getTable();
            return new Scan("", myTable.getDs(), Collections.emptyList());
        } else if (relNode instanceof LogicalFilter) {
            LogicalFilter filter = (LogicalFilter) relNode;
            LogicalPlan input = implement(filter.getInput());
            LogicalExpr condition = convertRexNode(filter.getCondition());
            return new Selection(input, condition);
        } else if (relNode instanceof LogicalProject) {
            LogicalProject project = (LogicalProject) relNode;
            LogicalPlan input = implement(project.getInput());
            List<LogicalExpr> projections = project.getProjects().stream()
                    .map(this::convertRexNode)
                    .collect(Collectors.toList());
            return new Projection(input, projections);
        }
        throw new UnsupportedOperationException("Unsupported RelNode: " + relNode.getClass().getName());
    }

    private LogicalExpr convertRexNode(RexNode rexNode) {
        if (rexNode instanceof RexCall) {
            RexCall call = (RexCall) rexNode;
            if (call.getOperator() == SqlStdOperatorTable.EQUALS) {
                LogicalExpr left = convertRexNode(call.getOperands().get(0));
                LogicalExpr right = convertRexNode(call.getOperands().get(1));
                return new Eq(left, right);
            }
        } else if (rexNode instanceof RexInputRef) {
            RexInputRef inputRef = (RexInputRef) rexNode;
            return new Column(inputRef.getName());
        } else if (rexNode instanceof RexLiteral) {
            RexLiteral literal = (RexLiteral) rexNode;
            if (literal.getTypeName() == SqlTypeName.INTEGER) {
                return new LiteralInt(((BigDecimal) literal.getValue()).intValue());
            }
        }
        throw new UnsupportedOperationException("Unsupported RexNode: " + rexNode.getClass().getName());
    }
}
