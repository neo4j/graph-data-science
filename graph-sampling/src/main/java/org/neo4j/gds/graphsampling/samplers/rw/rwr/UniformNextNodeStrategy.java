/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.gds.graphsampling.samplers.rw.rwr;

import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.core.utils.paged.HugeAtomicDoubleArray;
import org.neo4j.gds.graphsampling.samplers.rw.NextNodeStrategy;

import java.util.Optional;
import java.util.SplittableRandom;

public class UniformNextNodeStrategy implements NextNodeStrategy {

    protected final SplittableRandom rng;
    protected final Graph inputGraph;
    private final Optional<HugeAtomicDoubleArray> totalWeights;

    UniformNextNodeStrategy(
        SplittableRandom rng,
        Graph inputGraph,
        Optional<HugeAtomicDoubleArray> totalWeights
    ) {
        this.rng = rng;
        this.inputGraph = inputGraph;
        this.totalWeights = totalWeights;
    }

    @Override
    public long getNextNode(long currentNode) {
        if (totalWeights.isPresent()) {
            currentNode = weightedNextNode(currentNode);
        } else {
            int targetOffset = rng.nextInt(inputGraph.degree(currentNode));
            currentNode = inputGraph.nthTarget(currentNode, targetOffset);
            assert currentNode != IdMap.NOT_FOUND : "The offset '" + targetOffset + "' is bound by the degree but no target could be found for nodeId " + currentNode;
        }
        return currentNode;
    }

    long weightedNextNode(long currentNode) {
        var remainingMass = new MutableDouble(rng.nextDouble(0, computeDegree(currentNode)));
        var target = new MutableLong(RandomWalkWithRestarts.INVALID_NODE_ID);

        inputGraph.forEachRelationship(currentNode, 0.0, (src, trg, weight) -> {
            if (remainingMass.doubleValue() < weight) {
                target.setValue(trg);
                return false;
            }
            remainingMass.subtract(weight);
            return true;
        });

        assert target.getValue() != -1;

        return target.getValue();
    }

    protected double computeDegree(long currentNode) {
        if (totalWeights.isEmpty()) {
            return inputGraph.degree(currentNode);
        }

        var presentTotalWeights = totalWeights.get();
        if (presentTotalWeights.get(currentNode) == RandomWalkWithRestarts.TOTAL_WEIGHT_MISSING) {
            var degree = new MutableDouble(0.0);
            inputGraph.forEachRelationship(currentNode, 0.0, (src, trg, weight) -> {
                degree.add(weight);
                return true;
            });
            presentTotalWeights.set(currentNode, degree.doubleValue());
        }

        return presentTotalWeights.get(currentNode);
    }
}
