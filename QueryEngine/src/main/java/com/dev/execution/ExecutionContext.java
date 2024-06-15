package com.dev.execution;

import com.dev.datasource.InMemoryDataSource;
import com.dev.logical.plan.DataFrame;
import com.dev.logical.plan.DataFrameImpl;
import com.dev.logical.plan.Scan;

import javax.xml.crypto.Data;
import java.util.Collections;

public class ExecutionContext {

    public DataFrame inMemory() {
        return new DataFrameImpl(new Scan("", InMemoryDataSource.getInMemoryDataSourceInstanceWithDummyData(), Collections.emptyList()));
    }
}
