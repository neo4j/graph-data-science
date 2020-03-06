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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

public class CommunityAffiliations {
    private final long totalDoubleEdgeCount;
    private final List<SparseVector> affiliationVectors;
    private final Graph graph;
    private SparseVector affiliationSum;

    CommunityAffiliations(long totalDoubleEdgeCount, List<SparseVector> affiliationVectors, Graph graph) {
        this.totalDoubleEdgeCount = totalDoubleEdgeCount;
        this.affiliationVectors = affiliationVectors;
        this.graph = graph;
        this.affiliationSum = SparseVector.sum(affiliationVectors);
    }

    GainFunction blockGain(int nodeId) {
        return new AffiliationBlockGain(this, graph, nodeId);
    }

    SparseVector affiliationSum() {
        return this.affiliationSum;
    }

    public SparseVector nodeAffiliations(int nodeId) {
        return affiliationVectors.get(nodeId);
    }

    void updateNodeAffiliations(int nodeU, SparseVector increment) {
        affiliationVectors.set(nodeU, affiliationVectors.get(nodeU).add(increment));
        affiliationSum = affiliationSum.add(increment);
    }

    public double gain() {
        // 2*sum_U sum_V<U (log(1-exp(-vU.vV)) +vU.vV) + sum_U vU.vU - affSum.affSum
        double[] gain = new double[1];
        gain[0] = -affiliationSum.l2();
        for (int nodeU = 0; nodeU < graph.nodeCount(); nodeU++) {
            graph.forEachRelationship(nodeU, (src, trg) -> {
                SparseVector affiliationVector = nodeAffiliations((int) src);
                gain[0] += affiliationVector.l2();
                SparseVector neighborAffiliationVector = nodeAffiliations((int) trg);
                if (src < trg) {
                    return true;
                }
                double affiliationInnerProduct = affiliationVector.innerProduct(neighborAffiliationVector);
                gain[0] += 2*(Math.log(1 - Math.exp(-affiliationInnerProduct)) + affiliationInnerProduct);
                return true;
            });
        }
        return gain[0];
    }

    public long nodeCount() {
        return graph.nodeCount();
    }

    private double getEpsilon() {
        return BigDecimal
            .valueOf(totalDoubleEdgeCount)
            .divide(BigDecimal.valueOf(graph.nodeCount() * (graph.nodeCount() - 1)), 12, RoundingMode.FLOOR)
            .doubleValue();
    }

    public double getDelta() {
        return Math.sqrt(-Math.log(1 - getEpsilon()));
    }
}
