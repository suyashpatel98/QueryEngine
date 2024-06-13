package com.dev.datatypes;

import org.apache.arrow.vector.types.pojo.ArrowType;

class LiteralValueVector implements ColumnVector {
    private final ArrowType arrowType;
    private final Object value;
    private final int size;

    public LiteralValueVector(ArrowType arrowType, Object value, int size) {
        this.arrowType = arrowType;
        this.value = value;
        this.size = size;
    }

    @Override
    public ArrowType getType() {
        return arrowType;
    }

    @Override
    public Object getValue(int i) {
        if (i < 0 || i >= size) {
            throw new IndexOutOfBoundsException();
        }
        return value;
    }

    @Override
    public int size() {
        return size;
    }
}