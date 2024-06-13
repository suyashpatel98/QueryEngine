package com.dev.datatypes;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.ArrowType;

public class FieldVectorFactory {

    public static FieldVector create(ArrowType arrowType, int initialCapacity) {
        RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
        FieldVector fieldVector;
        if (arrowType.equals(ArrowTypes.BooleanType)) {
            fieldVector = new BitVector("v", rootAllocator);
        } else if (arrowType.equals(ArrowTypes.Int8Type)) {
            fieldVector = new TinyIntVector("v", rootAllocator);
        } else if (arrowType.equals(ArrowTypes.Int16Type)) {
            fieldVector = new SmallIntVector("v", rootAllocator);
        } else if (arrowType.equals(ArrowTypes.Int32Type)) {
            fieldVector = new IntVector("v", rootAllocator);
        } else if (arrowType.equals(ArrowTypes.Int64Type)) {
            fieldVector = new BigIntVector("v", rootAllocator);
        } else if (arrowType.equals(ArrowTypes.FloatType)) {
            fieldVector = new Float4Vector("v", rootAllocator);
        } else if (arrowType.equals(ArrowTypes.DoubleType)) {
            fieldVector = new Float8Vector("v", rootAllocator);
        } else if (arrowType.equals(ArrowTypes.StringType)) {
            fieldVector = new VarCharVector("v", rootAllocator);
        } else {
            throw new IllegalStateException();
        }
        if (initialCapacity != 0) {
            fieldVector.setInitialCapacity(initialCapacity);
        }
        fieldVector.allocateNew();
        return fieldVector;
    }
}
