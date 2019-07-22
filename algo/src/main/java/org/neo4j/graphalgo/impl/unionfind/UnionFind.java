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
package org.neo4j.graphalgo.impl.unionfind;

import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.HugeWeightMapping;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.graphalgo.core.utils.paged.dss.IncrementalDisjointSetStruct;
import org.neo4j.graphalgo.core.utils.paged.dss.RankedDisjointSetStruct;
import org.neo4j.graphalgo.core.utils.paged.dss.UnionStrategy;

import java.util.concurrent.ForkJoinTask;

public abstract class UnionFind<ME extends UnionFind<ME>> extends Algorithm<ME> {

    protected Graph graph;

    protected final UnionFind.Config algoConfig;

    static MemoryEstimation memoryEstimation(
            final boolean incremental,
            Class<? extends UnionFind> unionFindClass,
            Class<?> taskClass) {
        return MemoryEstimations.builder(unionFindClass)
                .startField("computeStep", taskClass)
                .add(MemoryEstimations.of("DisjointSetStruct", (dimensions, concurrency) -> {
                    MemoryEstimation dssEstimation = (incremental) ?
                            IncrementalDisjointSetStruct.memoryEstimation() :
                            RankedDisjointSetStruct.memoryEstimation();
                    return dssEstimation
                            .estimate(dimensions, concurrency)
                            .memoryUsage()
                            .times(concurrency);
                }))
                .endField()
                .build();
    }

    protected UnionFind(final Graph graph, final UnionFind.Config algoConfig) {
        this.graph = graph;
        this.algoConfig = algoConfig;
    }

    public double threshold() {
        return algoConfig.threshold;
    }

    DisjointSetStruct initDisjointSetStruct(long nodeCount, AllocationTracker tracker) {
        UnionStrategy unionStrategy = algoConfig.isUnionByRank ?
                new UnionStrategy.ByRank(nodeCount, tracker) :
                new UnionStrategy.ByMin();

        return algoConfig.communityMap == null ?
                new RankedDisjointSetStruct(nodeCount, unionStrategy, tracker) :
                new IncrementalDisjointSetStruct(nodeCount, algoConfig.communityMap, unionStrategy, tracker);
    }

    public DisjointSetStruct compute() {
        return Double.isFinite(threshold()) ? compute(threshold()) : computeUnrestricted();
    }

    public abstract DisjointSetStruct compute(double threshold);

    public abstract DisjointSetStruct computeUnrestricted();

    @Override
    public ME me() {
        //noinspection unchecked
        return (ME) this;
    }

    @Override
    public ME release() {
        graph = null;
        return me();
    }

    public static class Config {

        public final HugeWeightMapping communityMap;
        public final double threshold;
        public final boolean isUnionByRank;

        public Config(final HugeWeightMapping communityMap, final double threshold) {
            this(communityMap, threshold, true);
        }

        public Config(
                final HugeWeightMapping communityMap,
                final double threshold,
                final boolean isUnionByRank) {
            this.communityMap = communityMap;
            this.threshold = threshold;
            this.isUnionByRank = isUnionByRank;
        }
    }
}
