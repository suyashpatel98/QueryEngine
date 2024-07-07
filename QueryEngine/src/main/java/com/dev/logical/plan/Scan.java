package com.dev.logical.plan;

import com.dev.datasource.DataSource;
import com.dev.datatypes.Schema;

import java.util.List;

public class Scan implements LogicalPlan {
    private final String path;
    private final DataSource dataSource;
    private final List<String> projection;
    private final Schema schema;

    public Scan(String path, DataSource dataSource, List<String> projection) {
        this.path = path;
        this.dataSource = dataSource;
        this.projection = projection;
        this.schema = deriveSchema();
    }

    @Override
    public Schema schema() {
        return schema;
    }

    private Schema deriveSchema() {
        Schema schema = dataSource.schema();
        if (projection.isEmpty()) {
            return schema;
        } else {
            return schema.select(projection);
        }
    }

    @Override
    public List<LogicalPlan> children() {
        return List.of();
    }

    @Override
    public String toString() {
        if (projection.isEmpty()) {
            return "Scan: " + path + "; projection=None";
        } else {
            return "Scan: " + path + "; projection=" + projection;
        }
    }

    public String getPath() {
        return path;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public List<String> getProjection() {
        return projection;
    }

    public Schema getSchema() {
        return schema;
    }
}
