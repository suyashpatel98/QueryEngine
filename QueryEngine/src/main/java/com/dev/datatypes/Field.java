package com.dev.datatypes;

import org.apache.arrow.vector.types.pojo.ArrowType;

import java.util.ArrayList;

public class Field {
    private final String name;
    private final ArrowType dataType;

    public ArrowType getDataType() {
        return dataType;
    }

    public Field(String name, ArrowType dataType) {
        this.name = name;
        this.dataType = dataType;
    }

    public String getName() {
        return name;
    }

    public org.apache.arrow.vector.types.pojo.Field toArrow() {
        org.apache.arrow.vector.types.pojo.FieldType fieldType = new org.apache.arrow.vector.types.pojo.FieldType(true, dataType, null);
        return new org.apache.arrow.vector.types.pojo.Field(name, fieldType, new ArrayList<>());
    }
}
