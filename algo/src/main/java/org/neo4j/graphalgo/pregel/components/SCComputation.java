package org.neo4j.graphalgo.pregel.components;

import org.neo4j.graphalgo.pregel.Computation;

public class SCComputation extends Computation {

    @Override
    protected void compute(final long nodeId) {
        if (getSuperStep() == 0) {
            setValue(nodeId, nodeId);
            sendMessages(nodeId, nodeId);
        } else {
            long oldComponentId = (long) getValue(nodeId);
            long newComponentId = oldComponentId;

            for (double message : receiveMessages(nodeId)) {
                if (message < newComponentId) {
                    newComponentId = (long) message;
                }
            }

            if (newComponentId != oldComponentId) {
                setValue(nodeId, newComponentId);
                sendMessages(nodeId, newComponentId);
            }
        }
    }
}
