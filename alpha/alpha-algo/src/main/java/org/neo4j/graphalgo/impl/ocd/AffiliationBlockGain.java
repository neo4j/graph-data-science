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

import static org.neo4j.graphalgo.impl.ocd.CommunityAffiliations.LAMBDA;

public class AffiliationBlockGain implements GainFunction {
    private final int nodeU;
    private final CommunityAffiliations communityAffiliations;
    private final Graph graph;
    private final double deltaSquared;

    public AffiliationBlockGain(
        CommunityAffiliations communityAffiliations,
        Graph graph,
        int nodeU,
        double delta
    ) {
        this.nodeU = nodeU;
        this.graph = graph;
        this.communityAffiliations = communityAffiliations;
        this.deltaSquared = delta * delta;
    }

    @Override
    public double gain() {
        return gain(SparseVector.zero());
    }

    @Override
    public double gain(SparseVector increment) {
        // sum_V->U [ log(1 - exp(-A.Fv)) + A.Fv ] - A.affSum + A.Fu
        double[] gain = new double[1];
        SparseVector affiliationVector = communityAffiliations.nodeAffiliations(nodeU).add(increment);
        SparseVector affiliationSum = communityAffiliations.affiliationSum().add(increment);
        gain[0] = -affiliationVector.innerProduct(affiliationSum) - graph.nodeCount() * deltaSquared;
        gain[0] += affiliationVector.l2() + deltaSquared;
        graph.forEachRelationship(nodeU, (src, trg) -> {
            SparseVector neighborAffiliationVector = communityAffiliations.nodeAffiliations((int) trg);
            double affiliationInnerProduct = affiliationVector.innerProduct(neighborAffiliationVector) + deltaSquared;
            if (affiliationInnerProduct < 0) {
                gain[0] = Double.NEGATIVE_INFINITY;
                return false;
            }
            gain[0] += Math.log(1 - Math.exp(-affiliationInnerProduct)) + affiliationInnerProduct;
            return true;
        });
        double penalty = -LAMBDA * affiliationVector.l1();
        return gain[0] + penalty;
    }

    @Override
    public SparseVector gradient() {
        SparseVector Fu = communityAffiliations.nodeAffiliations(nodeU);
        List<SparseVector> gradientTerms = new LinkedList<>();
        graph.forEachRelationship(nodeU, (src, trg) -> {
            gradientTerms.add(weightedNeighbor(Fu, communityAffiliations.nodeAffiliations((int) trg)));
            return true;
        });
        gradientTerms.add(communityAffiliations.nodeAffiliations(nodeU));
        gradientTerms.add(communityAffiliations.affiliationSum().negate());
        return SparseVector.sum(gradientTerms).add(Fu.l1Gradient().multiply(-LAMBDA));
    }

    private SparseVector weightedNeighbor(SparseVector vU, SparseVector vV) {
        double innerProduct = - vU.innerProduct(vV) - deltaSquared;
        double expProduct = Math.exp(innerProduct);
        return vV.multiply(1 / (1 - expProduct));
    }
}
