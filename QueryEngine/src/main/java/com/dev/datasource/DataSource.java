package com.dev.datasource;

import com.dev.datatypes.RecordBatch;
import com.dev.datatypes.Schema;


import java.io.FileNotFoundException;
import java.util.List;

public interface DataSource {
    /**
     * Return the schema for the underlying data source.
     *
     * @return The schema for the underlying data source.
     */
    Schema schema();

    /**
     * Scan the data source, selecting the specified columns.
     *
     * @param projection List of column names to project.
     * @return Sequence of RecordBatch instances.
     */
    Iterable<RecordBatch> scan(List<String> projection) throws FileNotFoundException;
}
