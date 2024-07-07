package com.dev.physical.plan;

import com.dev.datatypes.*;

import java.io.FileNotFoundException;
import java.util.*;

import java.io.FileNotFoundException;
import java.util.List;

public interface PhysicalPlan {
    Schema getSchema();
    Iterable<RecordBatch> execute() throws FileNotFoundException;
    List<PhysicalPlan> getChildren();
}
