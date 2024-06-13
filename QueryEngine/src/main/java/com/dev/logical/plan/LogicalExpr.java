package com.dev.logical.plan;

import com.dev.datatypes.Field;

import java.sql.SQLException;
import java.util.stream.Collectors;

public abstract class LogicalExpr {
    abstract public Field toField(LogicalPlan input) throws SQLException;
}