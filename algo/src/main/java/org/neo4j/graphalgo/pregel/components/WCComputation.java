package org.neo4j.graphalgo.pregel.components;

import org.neo4j.graphalgo.pregel.Computation;

public class WCComputation extends Computation {

    @Override
    protected void compute(final long nodeId) {
        if (getSuperStep() == 0) {
            setValue(nodeId, nodeId);
            sendToNeighbors(nodeId, nodeId);
        } else {
            double[] messages = getMessages(nodeId);
            double newComponentId = (long) getValue(nodeId);
            for (double message : messages) {
                if (message < newComponentId) {
                    newComponentId = message;
                }
            }
            setValue(nodeId, newComponentId);
            sendToNeighbors(nodeId, newComponentId);
        }
    }
}
