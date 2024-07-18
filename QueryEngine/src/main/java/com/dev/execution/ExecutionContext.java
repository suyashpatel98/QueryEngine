package com.dev.execution;

import com.dev.datasource.InMemoryDataSource;
import com.dev.datatypes.RecordBatch;
import com.dev.logical.plan.DataFrame;
import com.dev.logical.plan.DataFrameImpl;
import com.dev.logical.plan.LogicalPlan;
import com.dev.logical.plan.Scan;
import com.dev.physical.plan.PhysicalPlan;
import com.dev.physical.plan.QueryPlanner;

import javax.swing.text.html.Option;
import javax.xml.crypto.Data;
import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;

public class ExecutionContext {

    public DataFrame inMemory() {
        return new DataFrameImpl(new Scan("", InMemoryDataSource.getInMemoryDataSourceInstanceWithDummyData(), Collections.emptyList()));
    }

    public Iterable<RecordBatch> execute(LogicalPlan plan) {
        try {
            PhysicalPlan physicalPlan = new QueryPlanner().createPhysicalPlan(plan);
            return physicalPlan.execute();
        } catch (Exception e) {
            System.out.println(e);
            return new ArrayList<>();
        }
    }
}
