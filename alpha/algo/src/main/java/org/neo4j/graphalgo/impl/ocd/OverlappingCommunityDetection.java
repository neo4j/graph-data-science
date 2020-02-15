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

import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class OverlappingCommunityDetection extends Algorithm<OverlappingCommunityDetection, OverlappingCommunityDetection> {
    private final List<SparseVector> affiliationVectors;
    private final Graph graph;
    private final GradientOptimizer optimizer;

    OverlappingCommunityDetection(Graph graph, AffiliationInitializer initializer, GradientOptimizer optimizer) {
        affiliationVectors = initializer.initialize(graph);
        this.graph = graph;
        this.optimizer = optimizer;
    }

    OverlappingCommunityDetection defaultOverlappingCommunityDetection(Graph graph) {
        double learningRate = 0.001;
        double tolerance = 0.00001;
        return new OverlappingCommunityDetection(graph, null, new GradientDescent(learningRate, tolerance));
    }

    @Override
    public OverlappingCommunityDetection compute() {
        while (optimizer.isRunning()) {
            optimizer.update(affiliationVectors, gradients());
        }
        return this;
    }

    @Override
    public OverlappingCommunityDetection me() {
        return this;
    }

    @Override
    public void release() {

    }

    public SparseVector affiliationSum() {
        return SparseVector.sum(affiliationVectors);
    }

    public double loss() {
        // 2*sum_U sum_V<U (log(1-exp(-vU.vV)) +vU.vV) + sum_U vU.vU - affSum.affSum
        SparseVector affiliationSum = affiliationSum();
        double[] loss = new double[1];
        loss[0] = -affiliationSum.l2();
        for (int nodeU = 0; nodeU < affiliationVectors.size(); nodeU++) {
            graph.forEachRelationship(nodeU, (src, trg) -> {
                SparseVector affiliationVector = affiliationVectors.get((int) src);
                loss[0] += affiliationVector.l2();
                SparseVector neighborAffiliationVector = affiliationVectors.get((int) trg);
                if (src < trg) {
                    return true;
                }
                double affiliationInnerProduct = affiliationVector.innerProduct(neighborAffiliationVector);
                loss[0] += 2*(Math.log(1 - Math.exp(-affiliationInnerProduct)) + affiliationInnerProduct);
                return true;
            });
        }
        return loss[0];
    }

    public SparseVector gradient(int nodeU, SparseVector negatedAffiliationSum) {
        SparseVector minusFU = affiliationVectors.get(nodeU).negate();
        List<SparseVector> gradientTerms = new LinkedList<>();
        graph.forEachRelationship(nodeU, (src, trg) -> {
            gradientTerms.add(weightedNeighbor(minusFU, affiliationVectors.get((int) trg)));
            return true;
        });
        gradientTerms.add(affiliationVectors.get(nodeU));
        gradientTerms.add(negatedAffiliationSum);
        return SparseVector.sum(gradientTerms);
    }

    public List<SparseVector> gradients() {
        SparseVector negatedAffiliationSum = affiliationSum().negate();
        return IntStream
            .of(affiliationVectors.size())
            .mapToObj((int nodeU) -> gradient(nodeU, negatedAffiliationSum))
            .collect(Collectors.toCollection(ArrayList::new));
    }

    public SparseVector weightedNeighbor(SparseVector vU, SparseVector vV) {
        double innerProduct = vU.innerProduct(vV);
        double expProduct = Math.exp(innerProduct);
        return vV.multiply(1 / (1 - expProduct));
    }
}
