package org.neo4j.graphalgo.pregel.pagerank;

import org.neo4j.graphalgo.pregel.Computation;

public class PRComputation extends Computation {

    private final long nodeCount;
    private final float jumpProbability;
    private final float dampingFactor;

    public PRComputation(final long nodeCount, final float jumpProbability, final float dampingFactor) {
        this.nodeCount = nodeCount;
        this.jumpProbability = jumpProbability;
        this.dampingFactor = dampingFactor;
    }

    @Override
    protected void compute(final long nodeId) {
        // init
        if (getSuperstep() == 0) {
            setValue(nodeId, 1.0 / nodeCount);
        }

        // compute new rank based on neighbor ranks
        if (getSuperstep() > 0) {
            double sum = 0;
            for (double message : receiveMessages(nodeId)) {
                sum += message;
            }
            setValue(nodeId, (jumpProbability / nodeCount) + dampingFactor * sum);
        }

        // send new rank to neighbors
        sendMessages(nodeId, getValue(nodeId) / getDegree(nodeId));

    }
}
