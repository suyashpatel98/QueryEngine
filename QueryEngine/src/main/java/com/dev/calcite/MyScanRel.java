package com.dev.calcite;

import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;

import java.util.List;

public class MyScanRel extends TableScan {
    private final MyTable myTable;

    public MyScanRel(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, MyTable myTable) {
        super(cluster, traitSet, table);
        this.myTable = myTable;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.isEmpty();
        return new MyScanRel(getCluster(), traitSet, table, myTable);
    }
}
