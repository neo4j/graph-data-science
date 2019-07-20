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

import org.neo4j.graphalgo.pregel.Computation;

public class PRComputation extends Computation {

    private final long nodeCount;
    private final float jumpProbability;
    private final float dampingFactor;

    public PRComputation(
            final long nodeCount,
            final float jumpProbability,
            final float dampingFactor) {
        this.nodeCount = nodeCount;
        this.jumpProbability = jumpProbability;
        this.dampingFactor = dampingFactor;
    }

    @Override
    protected void compute(final long nodeId) {
        double newRank = getValue(nodeId);

        // init
        if (getSuperstep() == 0) {
            newRank = 1.0 / nodeCount;
        }

        // compute new rank based on neighbor ranks
        if (getSuperstep() > 0) {
            double sum = 0;
            for (double message : receiveMessages(nodeId)) {
                sum += message;
            }
            newRank = (jumpProbability / nodeCount) + dampingFactor * sum;
        }

        // send new rank to neighbors
        setValue(nodeId, newRank);
        sendMessages(nodeId, newRank / getDegree(nodeId));
    }
}
