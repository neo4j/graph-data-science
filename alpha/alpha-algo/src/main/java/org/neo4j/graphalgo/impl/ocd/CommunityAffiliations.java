/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.impl.ocd;

import org.neo4j.graphalgo.api.Graph;

import java.util.List;

public class CommunityAffiliations {
    private final List<SparseVector> affiliationVectors;
    private final Graph graph;
    private SparseVector affiliationSum;

    CommunityAffiliations(List<SparseVector> affiliationVectors, Graph graph) {
        this.affiliationVectors = affiliationVectors;
        this.graph = graph;
        this.affiliationSum = SparseVector.sum(affiliationVectors);
    }

    LossFunction blockLoss(int nodeId) {
        return new AffiliationBlockLoss(this, graph, nodeId);
    }

    SparseVector affiliationSum() {
        return this.affiliationSum;
    }

    SparseVector nodeAffiliations(int nodeId) {
        return affiliationVectors.get(nodeId);
    }

    void updateNodeAffiliations(int nodeU, SparseVector increment) {
        affiliationVectors.set(nodeU, affiliationVectors.get(nodeU).add(increment));
        affiliationSum = affiliationSum.add(increment);
    }
}
