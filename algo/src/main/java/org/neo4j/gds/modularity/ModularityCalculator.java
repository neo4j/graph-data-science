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
package org.neo4j.gds.modularity;

import com.carrotsearch.hppc.cursors.LongLongCursor;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.paged.HugeAtomicDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongLongMap;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.Optional;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.function.LongUnaryOperator;

public class ModularityCalculator extends Algorithm<ModularityResult> {

    private final Graph graph;
    private final LongUnaryOperator communityIdProvider;
    private final HugeLongLongMap communityMapper;
    private final int concurrency;

    public static ModularityCalculator create(
        Graph graph,
        LongUnaryOperator seedCommunityIdProvider,
        int concurrency
    ) {
        var communityMapper = createMapping(graph.nodeCount(), seedCommunityIdProvider);
        LongUnaryOperator communityIdProvider = nodeId -> communityMapper.getOrDefault(
            seedCommunityIdProvider.applyAsLong(nodeId),
            -1
        );
        return new ModularityCalculator(graph, communityIdProvider, communityMapper, concurrency);
    }

    private ModularityCalculator(
        Graph graph,
        LongUnaryOperator communityIdProvider,
        HugeLongLongMap communityMapper,
        int concurrency
    ) {
        super(ProgressTracker.NULL_TRACKER);
        this.graph = graph;
        this.communityIdProvider = communityIdProvider;
        this.communityMapper = communityMapper;
        this.concurrency = concurrency;
    }

    @Override
    public ModularityResult compute() {
        var nodeCount = graph.nodeCount();

        var communityCount = communityMapper.size();
        var insideRelationships = HugeAtomicDoubleArray.newArray(communityCount);
        var totalCommunityRelationships = HugeAtomicDoubleArray.newArray(communityCount);
        var totalRelationshipWeight = new DoubleAdder();

        var tasks = PartitionUtils.rangePartition(
            concurrency,
            nodeCount,
            partition -> new RelationshipCountCollector(
                partition,
                graph,
                insideRelationships,
                totalCommunityRelationships,
                communityIdProvider,
                totalRelationshipWeight
            ), Optional.empty()
        );

        RunWithConcurrency.builder()
            .concurrency(concurrency)
            .tasks(tasks)
            .run();

        var communityModularities = HugeObjectArray.newArray(
            CommunityModularity.class,
            communityCount
        );
        var totalRelWeight = totalRelationshipWeight.doubleValue();
        var totalModularity = new MutableDouble();
        long resultIndex = 0;
        for (LongLongCursor cursor : communityMapper) {
            long communityId = cursor.key;
            long mappedCommunityId = cursor.value;
            var ec = insideRelationships.get(mappedCommunityId);
            var Kc = totalCommunityRelationships.get(mappedCommunityId);
            var modularity = (ec - Kc * Kc * (1.0 / totalRelWeight)) / totalRelWeight;
            totalModularity.add(modularity);
            communityModularities.set(resultIndex++, CommunityModularity.of(communityId, modularity));
        }

        return ModularityResult.of(totalModularity.doubleValue(), communityCount, communityModularities);
    }

    static HugeLongLongMap createMapping(long nodeCount, LongUnaryOperator seedCommunityId) {

        var seedMap = new HugeLongLongMap(nodeCount);
        long seedId = 0;
        for (long nodeId = 0; nodeId < nodeCount; ++nodeId) {
            long communityId = seedCommunityId.applyAsLong(nodeId);
            if (!seedMap.containsKey(communityId)) {
                seedMap.put(communityId, seedId++);
            }
        }
        return seedMap;
    }
}
