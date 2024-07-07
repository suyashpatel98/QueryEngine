package com.dev.datatypes;

import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.lang.IllegalStateException;


/** Wrapper around Arrow FieldVector */
public class ArrowFieldVector implements ColumnVector {

    private final FieldVector field;

    public ArrowFieldVector(FieldVector field) {
        this.field = field;
    }

    public FieldVector getField() {
        return field;
    }

    @Override
    public ArrowType getType() {
        if (field instanceof BitVector) {
            return ArrowTypes.BooleanType;
        } else if (field instanceof TinyIntVector) {
            return ArrowTypes.Int8Type;
        } else if (field instanceof SmallIntVector) {
            return ArrowTypes.Int16Type;
        } else if (field instanceof IntVector) {
            return ArrowTypes.Int32Type;
        } else if (field instanceof BigIntVector) {
            return ArrowTypes.Int64Type;
        } else if (field instanceof Float4Vector) {
            return ArrowTypes.FloatType;
        } else if (field instanceof Float8Vector) {
            return ArrowTypes.DoubleType;
        } else if (field instanceof VarCharVector) {
            return ArrowTypes.StringType;
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    public Object getValue(int i) {
        if (field.isNull(i)) {
            return null;
        }

        if (field instanceof BitVector) {
            BitVector bitVector = (BitVector) field;
            return bitVector.get(i) == 1;
        } else if (field instanceof TinyIntVector) {
            TinyIntVector tinyIntVector = (TinyIntVector) field;
            return tinyIntVector.get(i);
        } else if (field instanceof SmallIntVector) {
            SmallIntVector smallIntVector = (SmallIntVector) field;
            return smallIntVector.get(i);
        } else if (field instanceof IntVector) {
            IntVector intVector = (IntVector) field;
            return intVector.get(i);
        } else if (field instanceof BigIntVector) {
            BigIntVector bigIntVector = (BigIntVector) field;
            return bigIntVector.get(i);
        } else if (field instanceof Float4Vector) {
            Float4Vector float4Vector = (Float4Vector) field;
            return float4Vector.get(i);
        } else if (field instanceof Float8Vector) {
            Float8Vector float8Vector = (Float8Vector) field;
            return float8Vector.get(i);
        } else if (field instanceof VarCharVector) {
            VarCharVector varCharVector = (VarCharVector) field;
            byte[] bytes = varCharVector.get(i);
            return bytes == null ? null : new String(bytes);
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    public int size() {
        return field.getValueCount();
    }
}
