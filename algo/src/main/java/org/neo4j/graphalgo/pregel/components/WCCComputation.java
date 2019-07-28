package org.neo4j.graphalgo.pregel.components;

import org.neo4j.graphalgo.pregel.Computation;
import org.neo4j.graphdb.Direction;

public class WCCComputation extends Computation {

    @Override
    protected Direction getMessageDirection() {
        return Direction.BOTH;
    }

    @Override
    protected void compute(final long nodeId) {
        if (getSuperstep() == 0) {
            setValue(nodeId, nodeId);
            sendMessages(nodeId, nodeId, Direction.BOTH);
        } else {
            long oldComponentId = (long) getValue(nodeId);
            long newComponentId = oldComponentId;

            for (double message : receiveMessages(nodeId, Direction.BOTH)) {
                if (message < newComponentId) {
                    newComponentId = (long) message;
                }
            }

            if (newComponentId != oldComponentId) {
                setValue(nodeId, newComponentId);
                sendMessages(nodeId, newComponentId, Direction.BOTH);
            }
        }
    }
}
