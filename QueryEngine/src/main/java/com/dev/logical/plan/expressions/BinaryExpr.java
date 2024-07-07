package com.dev.logical.plan.expressions;


import com.dev.logical.plan.LogicalExpr;

public abstract class BinaryExpr extends LogicalExpr {
    protected final String name;
    private final String op;
    protected final LogicalExpr l;
    private final LogicalExpr r;

    protected BinaryExpr(String name, String op, LogicalExpr l, LogicalExpr r) {
        this.name = name;
        this.op = op;
        this.l = l;
        this.r = r;
    }

    public String getName() {
        return name;
    }

    public String getOp() {
        return op;
    }

    public LogicalExpr getL() {
        return l;
    }

    public LogicalExpr getR() {
        return r;
    }

    @Override
    public String toString() {

        return l + " " + op + " " + r;
    }
}
