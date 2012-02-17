package edu.brown.costmodel.samples;

import java.util.ArrayList;
import java.util.List;

import org.voltdb.plannodes.AbstractPlanNode;

public abstract class DistributedJoin {
    private static Integer node_id = 1;
    private static Integer dep_id = 1;
    private static final Integer num_of_scans = 2;

    public static final List<AbstractPlanNode> plans = new ArrayList<AbstractPlanNode>();
    /*
     * static { Stack<Integer> dep_ids = new Stack<Integer>(); // // IndexScan
     * // { SendPlanNode node0 = new SendPlanNode(node_id++);
     * node0.setDependencyId(dep_id++); dep_ids.push(node0.getDependencyId());
     * ProjectionPlanNode node1 = new ProjectionPlanNode(node_id++);
     * node0.getChildren().add(node1); node1.getParents().add(node0);
     * IndexScanPlanNode node2 = new IndexScanPlanNode(node_id++);
     * node1.getChildren().add(node2); node2.getParents().add(node1);
     * plans.add(node0); } // // SeqScans // for (int ctr = 0; ctr <
     * num_of_scans; ctr++) { SendPlanNode node0 = new SendPlanNode(node_id++);
     * node0.setDependencyId(dep_id++); dep_ids.push(node0.getDependencyId());
     * SeqScanPlanNode node1 = new SeqScanPlanNode(node_id++);
     * node0.getChildren().add(node1); node1.getParents().add(node0);
     * plans.add(node0); } // FOR // // NestLoop + Union // { SendPlanNode node0
     * = new SendPlanNode(node_id++); node0.setDependencyId(dep_id++);
     * UnionPlanNode node1 = new UnionPlanNode(node_id++); for (int ctr = 0; ctr
     * < num_of_scans; ctr++) { ReceivePlanNode node2 = new
     * ReceivePlanNode(node_id++); node2.setDependencyId(dep_ids.pop());
     * node1.getChildren().add(node2); node2.getParents().add(node1); } // FOR
     * ReceivePlanNode node2 = new ReceivePlanNode(node_id++);
     * node2.setDependencyId(dep_ids.pop()); NestLoopPlanNode node3 = new
     * NestLoopPlanNode(node_id++); node3.getChildren().add(node1);
     * node3.getChildren().add(node2); node0.getChildren().add(node3);
     * node1.getParents().add(node3); node2.getParents().add(node3);
     * plans.add(node0); } } // STATIC
     */
}
