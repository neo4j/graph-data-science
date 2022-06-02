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
package org.neo4j.gds.beta.modularity;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.paged.HugeAtomicDoubleArray;

import java.util.stream.LongStream;

interface ModularityOptimizationModularity {

    double getModularity();

    static ModularityOptimizationModularity createModularity(Graph graph, int concurrency) {
        if (graph.isUndirected()) {
            return new ModularityOptimizationUndirectedModularity(graph, concurrency);
        } else {
            return new ModularityOptimizationDirectedModularity(graph, concurrency);
        }
    }

    void setTotalWeight(double totalWeight);

    void communityWeightUpdate(long communityId, double weight);

    double getCommunityWeight(long communityId);

    void processMove(
        long oldCommunity,
        long nextCommunity,
        double oldInfluence,
        double newInfluence,
        double selfWeight
    );

    void processSeedContribution(long communityId, double weight);

}

class ModularityOptimizationUndirectedModularity implements ModularityOptimizationModularity {
    private final HugeAtomicDoubleArray globalCommunityInfluences;
    private final Graph graph;
    private final int concurrency;
    private double totalWeight;

    private final HugeAtomicDoubleArray communityWeights;

    ModularityOptimizationUndirectedModularity(Graph graph, int concurrency) {
        this.graph = graph;
        long nodeCount = graph.nodeCount();
        globalCommunityInfluences = HugeAtomicDoubleArray.newArray(nodeCount);
        communityWeights = HugeAtomicDoubleArray.newArray(nodeCount);

        this.concurrency = concurrency;
    }

    @Override
    public void setTotalWeight(double totalWeight) {
        this.totalWeight = totalWeight;
    }

    @Override
    public void processMove(
        long oldCommunity,
        long nextCommunity,
        double oldInfluence,
        double newInfluence,
        double selfWeight
    ) {
        if (oldCommunity != nextCommunity) {
            globalCommunityInfluences.getAndAdd(oldCommunity, -2 * oldInfluence + selfWeight);
            globalCommunityInfluences.getAndAdd(nextCommunity, 2 * newInfluence + selfWeight);
        }

    }

    @Override
    public void processSeedContribution(long communityId, double weight) {
        globalCommunityInfluences.getAndAdd(communityId, weight);
    }

    @Override
    public void communityWeightUpdate(long communityId, double weight) {
        communityWeights.update(communityId, agg -> agg + weight);

    }

    @Override
    public double getCommunityWeight(long communityId) {
        return communityWeights.get(communityId);
    }

    public double getModularity() {
        double ex = ParallelUtil.parallelStream(
            LongStream.range(0, graph.nodeCount()),
            concurrency,
            nodeStream ->
                nodeStream
                    .mapToDouble(globalCommunityInfluences::get)
                    .reduce(Double::sum)
                    .orElseThrow(() -> new RuntimeException("Error while computing modularity"))
        );

        double ax = ParallelUtil.parallelStream(
            LongStream.range(0, graph.nodeCount()),
            concurrency,
            nodeStream ->
                nodeStream
                    .mapToDouble(nodeId -> Math.pow(communityWeights.get(nodeId), 2.0))
                    .reduce(Double::sum)
                    .orElseThrow(() -> new RuntimeException("Error while computing modularity"))
        );

        return (ex / (totalWeight)) - (ax / (Math.pow(totalWeight, 2)));
    }
}

class ModularityOptimizationDirectedModularity implements ModularityOptimizationModularity {

    private final Graph graph;
    private double totalWeight;

    private final int concurrency;


    ModularityOptimizationDirectedModularity(Graph graph, int concurrency) {
        this.graph = graph;
        this.concurrency = concurrency;
    }

    public double getModularity() {
        return 0;
    }

    @Override
    public void setTotalWeight(double totalWeight) {
        this.totalWeight = totalWeight;

    }

    @Override
    public void communityWeightUpdate(long communityId, double weight) {

    }

    @Override
    public double getCommunityWeight(long communityId) {
        return 0;
    }

    @Override
    public void processMove(
        long oldCommunity,
        long nextCommunity,
        double oldInfluence,
        double newInfluence,
        double selfWeight
    ) {

    }

    @Override
    public void processSeedContribution(long communityId, double weight) {

    }
}
