package com.dev.physical.plan;

import com.dev.datasource.DataSource;
import com.dev.datatypes.RecordBatch;
import com.dev.datatypes.Schema;

import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import java.util.Collections;
import java.util.List;

public class ScanExec implements PhysicalPlan {
    private final DataSource ds;
    private final List<String> projection;

    public ScanExec(DataSource ds, List<String> projection) {
        this.ds = ds;
        this.projection = projection;
    }

    @Override
    public Schema getSchema() {
        return ds.schema().select(projection);
    }

    @Override
    public List<PhysicalPlan> getChildren() {
        // Scan is a leaf node and has no child plans
        return Collections.emptyList();
    }

    @Override
    public Iterable<RecordBatch> execute() throws FileNotFoundException {
        return ds.scan(projection);
    }

    @Override
    public String toString() {
        return "ScanExec: schema=" + getSchema() + ", projection=" + projection;
    }
}
