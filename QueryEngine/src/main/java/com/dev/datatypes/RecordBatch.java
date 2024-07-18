package com.dev.datatypes;

import java.util.*;

public class RecordBatch {
    private final Schema schema;
    private final List<ColumnVector> fields;

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for(ColumnVector field : fields) {
            sb.append(field.toString() + "\n");
        }
        return sb.toString();
    }

    public RecordBatch(Schema schema, List<ColumnVector> fields) {
        this.schema = schema;
        this.fields = fields;
    }

    public int rowCount() {
        return fields.get(0).size();
    }

    public int columnCount() {
        return fields.size();
    }

    public Schema getSchema() {
        return schema;
    }

    public List<ColumnVector> getFields() {
        return fields;
    }

    public ColumnVector getField(int i) {
        return fields.get(i);
    }

    /** Access one column by index */
    public ColumnVector field(int i) {
        return fields.get(i);
    }
}

