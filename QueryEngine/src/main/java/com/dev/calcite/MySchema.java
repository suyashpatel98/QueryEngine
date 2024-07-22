package com.dev.calcite;

import com.dev.datasource.DataSource;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.*;

public class MySchema extends AbstractSchema {
    private final DataSource ds;

    public MySchema(DataSource ds) {
        this.ds = ds;
    }

    @Override
    protected Map<String, Table> getTableMap() {
        return Collections.singletonMap("my_table", new MyTable(ds));
    }
}

