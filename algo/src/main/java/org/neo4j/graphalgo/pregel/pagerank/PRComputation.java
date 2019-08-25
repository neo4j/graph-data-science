/*
 * Copyright (c) 2017-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.pregel.pagerank;

import org.jctools.queues.MpscLinkedQueue;
import org.neo4j.graphalgo.pregel.Computation;

public class PRComputation extends Computation {

    private final long nodeCount;
    private final double jumpProbability;
    private final double dampingFactor;

    public PRComputation(
            final long nodeCount,
            final double dampingFactor) {
        this.nodeCount = nodeCount;
        this.jumpProbability = 1.0 - dampingFactor;
        this.dampingFactor = dampingFactor;
    }

    @Override
    protected double getDefaultNodeValue() {
        return 1.0 / nodeCount;
    }

    @Override
    protected void compute(final long nodeId, MpscLinkedQueue<Double> messages) {
        double newRank = getValue(nodeId);

        // compute new rank based on neighbor ranks
        if (getSuperstep() > 0) {
            double sum = 0;
            if (messages != null) {
                Double nextMessage;
                while (!(nextMessage = messages.poll()).isNaN()) {
                    sum += nextMessage;
                }
            }
            newRank = (jumpProbability / nodeCount) + dampingFactor * sum;
        }

        // send new rank to neighbors
        setValue(nodeId, newRank);
        sendMessages(nodeId, newRank / getDegree(nodeId));
    }
}
