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
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.paged.HugeAtomicDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.partition.PartitionUtils;

import java.util.Optional;
import java.util.stream.LongStream;


class ModularityManager {

    private final Graph graph;
    private final double totalWeight;
    private final HugeAtomicDoubleArray communityWeights;
    private HugeLongArray communities;
    private final int concurrency;


    static ModularityManager create(Graph graph, int concurrency, double totalWeight) {
        return new ModularityManager(
            graph,
            HugeAtomicDoubleArray.newArray(graph.nodeCount()),
            concurrency,
            totalWeight
        );
    }

    private ModularityManager(Graph graph, HugeAtomicDoubleArray communityWeights, int concurrency, double totalWeight) {
        this.graph = graph;
        this.communityWeights = communityWeights;
        this.concurrency = concurrency;
        this.totalWeight = totalWeight;
    }

    double calculateModularity() {
        HugeAtomicDoubleArray insideRelationships = HugeAtomicDoubleArray.newArray(graph.nodeCount());
        var tasks = PartitionUtils.rangePartition(
            concurrency,
            graph.nodeCount(),
            partition -> new InsideRelationshipCalculator(
                partition,
                graph,
                insideRelationships,
                communities
            ), Optional.empty()
        );

        RunWithConcurrency.builder()
            .concurrency(concurrency)
            .tasks(tasks)
            .run();

        double modularity = ParallelUtil.parallelStream(
            LongStream.range(0, graph.nodeCount()),
            concurrency,
            nodeStream ->
                nodeStream
                    .mapToDouble(communityId -> {
                        double ec = insideRelationships.get(communityId);
                        double Kc = communityWeights.get(communityId);
                        return ec - Kc * Kc * (1.0 / totalWeight);
                    })
                    .reduce(Double::sum)
                    .orElseThrow(() -> new RuntimeException("Error while computing modularity"))
        );
        return modularity * (1.0 / totalWeight);
    }

    void communityWeightUpdate(long communityId, double weight) {
        communityWeights.update(communityId, agg -> agg + weight);
    }

    double getCommunityWeight(long communityId) {
        return communityWeights.get(communityId);
    }

    void registerCommunities(HugeLongArray communities) {
        this.communities = communities;
    }

}
