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

import java.util.LinkedList;
import java.util.List;

public class AffiliationBlockGain implements GainFunction {
    private final int nodeU;
    private final CommunityAffiliations communityAffiliations;
    private final Graph graph;

    public AffiliationBlockGain(
        CommunityAffiliations communityAffiliations,
        Graph graph,
        int nodeU
    ) {
        this.nodeU = nodeU;
        this.graph = graph;
        this.communityAffiliations = communityAffiliations;
    }

    @Override
    public double gain(SparseVector affiliationVector) {
        // 2*sum_U sum_V<U (log(1-exp(-vU.vV)) +vU.vV) + sum_U vU.vU - affSum.affSum
        double[] loss = new double[1];
        loss[0] = -communityAffiliations.affiliationSum().l2();
        graph.forEachRelationship(nodeU, (src, trg) -> {
            loss[0] += affiliationVector.l2();
            SparseVector neighborAffiliationVector = communityAffiliations.nodeAffiliations((int) trg);
            double affiliationInnerProduct = affiliationVector.innerProduct(neighborAffiliationVector);
            loss[0] += Math.log(1 - Math.exp(-affiliationInnerProduct)) + affiliationInnerProduct;
            return true;
        });
        return loss[0];
    }

    @Override
    public SparseVector gradient() {
        SparseVector minusFU = communityAffiliations.nodeAffiliations(nodeU).negate();
        List<SparseVector> gradientTerms = new LinkedList<>();
        graph.forEachRelationship(nodeU, (src, trg) -> {
            gradientTerms.add(weightedNeighbor(minusFU, communityAffiliations.nodeAffiliations((int) trg)));
            return true;
        });
        gradientTerms.add(communityAffiliations.nodeAffiliations(nodeU));
        gradientTerms.add(communityAffiliations.affiliationSum().negate());
        return SparseVector.sum(gradientTerms);
    }

    @Override
    public SparseVector parameters() {
        return communityAffiliations.nodeAffiliations(nodeU);
    }

    @Override
    public void update(SparseVector increment) {
        communityAffiliations.updateNodeAffiliations(nodeU, increment);

    }

    private SparseVector weightedNeighbor(SparseVector vU, SparseVector vV) {
        double innerProduct = vU.innerProduct(vV);
        double expProduct = Math.exp(innerProduct);
        return vV.multiply(1 / (1 - expProduct));
    }
}
