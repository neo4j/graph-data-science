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

package org.neo4j.graphalgo.impl.wcc;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.PropertyMapping;
import org.neo4j.graphalgo.core.loading.NullPropertyMap;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphalgo.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.graphalgo.beta.pregel.Pregel;
import org.neo4j.graphalgo.beta.pregel.examples.WCCComputation;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.concurrent.ExecutorService;

public class WCCPregel extends WCC<WCCPregel> {

    private final Pregel pregel;
    private final long nodeCount;
    private final long batchSize;

    public static MemoryEstimation memoryEstimation(boolean incremental) {
        return MemoryEstimations
                .builder(WCCPregel.class)
                // TODO
                .build();
    }

    WCCPregel(
            Graph graph,
            ExecutorService executor,
            int minBatchSize,
            int concurrency,
            WCC.Config algoConfig,
            AllocationTracker tracker) {
        super(graph, algoConfig);
        this.nodeCount = graph.nodeCount();
        this.batchSize = ParallelUtil.adjustedBatchSize(
                nodeCount,
                concurrency,
                minBatchSize,
                Integer.MAX_VALUE);
        long threadSize = ParallelUtil.threadCount(batchSize, nodeCount);
        if (threadSize > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(String.format(
                    "Too many nodes (%d) to run union find with the given concurrency (%d) and batchSize (%d)",
                    nodeCount,
                    concurrency,
                    batchSize));
        }

        PropertyMapping communityMap = algoConfig.communityMap;

        if (communityMap == null || communityMap instanceof NullPropertyMap) {
            this.pregel = Pregel.withDefaultNodeValues(
                    graph,
                    WCCComputation::new,
                    (int) batchSize,
                    (int) threadSize,
                    executor,
                    tracker
            );
        } else {
            this.pregel = Pregel.withInitialNodeValues(
                    graph,
                    WCCComputation::new,
                    communityMap,
                    (int) batchSize,
                    (int) threadSize,
                    executor,
                    tracker
            );
        }
    }

    @Override
    public DisjointSetStruct computeUnrestricted() {
        return compute(Double.NaN);
    }

    @Override
    public DisjointSetStruct compute(double threshold) {
        final HugeDoubleArray communities = pregel.run(Integer.MAX_VALUE);

        return new DisjointSetStruct() {
            @Override
            public void union(final long p, final long q) {
                throw new NotImplementedException();
            }

            @Override
            public long setIdOf(final long nodeId) {
                return (long) communities.get(nodeId);
            }

            @Override
            public boolean sameSet(final long p, final long q) {
                return setIdOf(p) == setIdOf(q);
            }

            @Override
            public long size() {
                return communities.size();
            }
        };
    }
}
