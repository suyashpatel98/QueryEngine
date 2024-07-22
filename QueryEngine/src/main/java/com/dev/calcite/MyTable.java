package com.dev.calcite;

import com.dev.datasource.DataSource;
import com.dev.datatypes.*;
import com.dev.physical.plan.ScanExec;
import com.google.common.collect.Lists;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.*;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.TransientTable;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.io.FileNotFoundException;
import java.util.*;
import java.util.stream.Collectors;

public class MyTable extends AbstractTable implements ScannableTable, TranslatableTable {
    private final DataSource ds;

    public MyTable(DataSource ds) {
        this.ds = ds;
    }

    public DataSource getDs() {
        return ds;
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        return new MyScanRel(context.getCluster(), context.getCluster().traitSetOf(Convention.NONE), relOptTable, this);
    }


    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        // This method won't be used, but needs to be implemented
        throw new UnsupportedOperationException("Direct scan not supported");
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        List<RelDataType> fieldTypes = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();

        for (Field field : ds.schema().getFields()) {
            fieldNames.add(field.getName());
            fieldTypes.add(convertToSqlType(field.getDataType(), typeFactory));
        }

        return typeFactory.createStructType(fieldTypes, fieldNames);
    }

    private RelDataType convertToSqlType(ArrowType type, RelDataTypeFactory typeFactory) {
        // This method needs to be implemented based on your DataType
        // Here's a simple example, extend as needed
        if (type instanceof ArrowType.Int) {
            ArrowType.Int intType = (ArrowType.Int) type;
            switch (intType.getBitWidth()) {
                case 32:
                    return typeFactory.createSqlType(SqlTypeName.INTEGER);
                default:
                    throw new UnsupportedOperationException("Unsupported type: " + type);
            }
        } else if (type instanceof ArrowType.Utf8) {
            return typeFactory.createSqlType(SqlTypeName.VARCHAR);
        }
        throw new UnsupportedOperationException("Unsupported type: " + type);
    }
}

