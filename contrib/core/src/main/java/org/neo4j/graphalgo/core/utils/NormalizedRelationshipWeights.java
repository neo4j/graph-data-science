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
package org.neo4j.graphalgo.core.utils;

import com.carrotsearch.hppc.IntDoubleMap;
import com.carrotsearch.hppc.IntDoubleScatterMap;
import org.neo4j.graphalgo.api.NodeIterator;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.api.RelationshipWeights;
import org.neo4j.graphdb.Direction;

/**
 * @author mknblch
 */
public class NormalizedRelationshipWeights implements RelationshipWeights {

    private RelationshipWeights weights;
    private IntDoubleMap nodeWeightSum;

    public NormalizedRelationshipWeights(int nodeCount, RelationshipIterator relationshipIterator, RelationshipWeights weights) {
        this.weights = weights;
        this.nodeWeightSum = new IntDoubleScatterMap();
        for (int n = 0; n < nodeCount; n++) {
            relationshipIterator.forEachRelationship(n, Direction.OUTGOING, (s, t, r) -> {
                nodeWeightSum.addTo(s, weights.weightOf(s, t));
                return true;
            });
        }
    }

    @Override
    public double weightOf(int sourceNodeId, int targetNodeId) {
        return weights.weightOf(sourceNodeId, targetNodeId) / nodeWeightSum.getOrDefault(sourceNodeId, 1.);
    }

    public void release() {
        nodeWeightSum.clear();
        nodeWeightSum = null;
        weights = null;
    }
}
