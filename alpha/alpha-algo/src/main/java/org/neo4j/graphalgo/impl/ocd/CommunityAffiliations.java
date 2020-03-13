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
    private final List<Vector> affiliationVectors;
    private final Graph graph;
    private final Vector affiliationSum;
    static final double LAMBDA = 0.1;
    private final Vector l1PenaltyGradient;

    CommunityAffiliations(long totalDoubleEdgeCount, List<Vector> affiliationVectors, Graph graph) {
        this.totalDoubleEdgeCount = totalDoubleEdgeCount;
        this.affiliationVectors = affiliationVectors;
        this.graph = graph;
        this.affiliationSum = Vector.sum(affiliationVectors);
        this.l1PenaltyGradient = Vector.l1PenaltyGradient(affiliationSum.dim(), LAMBDA);
    }

    GainFunction blockGain(int nodeId, double delta) {
        return new AffiliationBlockGain(this, graph, nodeId, delta);
    }

    Vector affiliationSum() {
        return this.affiliationSum;
    }

    Vector l1PenaltyGradient() {
        return l1PenaltyGradient;
    }

    public Vector nodeAffiliations(int nodeId) {
        return affiliationVectors.get(nodeId);
    }

    synchronized void updateNodeAffiliations(int nodeU, Vector increment) {
        Vector newVector = affiliationVectors.get(nodeU).addAndProject(increment);
        Vector diff = newVector.subtract(affiliationVectors.get(nodeU));
        affiliationVectors.set(nodeU, newVector);
        affiliationSum.addInPlace(diff);
    }

    double gain() {
        // 2*sum_U sum_V<U (log(1-exp(-vU.vV)) +vU.vV) + sum_U vU.vU - affSum.affSum
        double delta = getDelta();
        double deltaSquared = delta * delta;
        double totalDeltaSquared = (graph.nodeCount() * delta) * (graph.nodeCount() * delta);
        double[] gain = new double[1];
        gain[0] = -affiliationSum.l2Squared() - totalDeltaSquared;
        double[] l1Penalty = new double[1];
        l1Penalty[0] = 0;
        for (int nodeU = 0; nodeU < graph.nodeCount(); nodeU++) {
            Vector affiliationVector = nodeAffiliations(nodeU);
            l1Penalty[0] += affiliationVector.l1();
            gain[0] += affiliationVector.l2Squared() + deltaSquared;
            graph.forEachRelationship(nodeU, (src, trg) -> {
                if (src < trg) {
                    return true;
                }
                Vector neighborAffiliationVector = nodeAffiliations((int) trg);
                double affiliationInnerProduct = affiliationVector.innerProduct(neighborAffiliationVector) + deltaSquared;
                gain[0] += 2*(Math.log(1 - Math.exp(-affiliationInnerProduct)) + affiliationInnerProduct);
                return true;
            });
        }
        return gain[0] - LAMBDA * l1Penalty[0];
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
