package com.dev.datatypes;

import org.apache.arrow.vector.types.pojo.ArrowType;

import java.util.*;
import java.util.stream.Collectors;

class SchemaConverter {
    public static Schema fromArrow(org.apache.arrow.vector.types.pojo.Schema arrowSchema) {
        List<Field> fields = arrowSchema.getFields().stream()
                .map(f -> new Field(f.getName(), f.getFieldType().getType()))
                .collect(Collectors.toList());
        return new Schema(fields);
    }
}

public class Schema {
    private final List<Field> fields;

    public Schema(List<Field> fields) {
        this.fields = fields;
    }

    public org.apache.arrow.vector.types.pojo.Schema toArrow() {
        List<org.apache.arrow.vector.types.pojo.Field> arrowFields = fields.stream()
                .map(Field::toArrow)
                .collect(Collectors.toList());
        return new org.apache.arrow.vector.types.pojo.Schema(arrowFields);
    }

    public Schema project(List<Integer> indices) {
        List<Field> projectedFields = indices.stream()
                .map(i -> fields.get(i))
                .collect(Collectors.toList());
        return new Schema(projectedFields);
    }

    public Schema select(List<String> names) {
        List<Field> selectedFields = new ArrayList<>();
        for (String name : names) {
            List<Field> matchingFields = fields.stream()
                    .filter(f -> f.getName().equals(name))
                    .collect(Collectors.toList());
            if (matchingFields.size() == 1) {
                selectedFields.add(matchingFields.get(0));
            } else {
                throw new IllegalArgumentException();
            }
        }
        return new Schema(selectedFields);
    }

    public List<Field> getFields() {
        return fields;
    }
}


