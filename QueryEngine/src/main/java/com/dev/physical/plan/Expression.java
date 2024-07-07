package com.dev.physical.plan;

import com.dev.datatypes.ColumnVector;
import com.dev.datatypes.RecordBatch;

public interface Expression {
    ColumnVector evaluate(RecordBatch input);
}
