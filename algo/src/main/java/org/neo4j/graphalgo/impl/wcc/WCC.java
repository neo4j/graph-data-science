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

import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.PropertyMapping;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.graphalgo.core.utils.paged.dss.IncrementalDisjointSetStruct;
import org.neo4j.graphalgo.core.utils.paged.dss.NonInrementalDisjointSetStruct;
import org.neo4j.graphalgo.core.utils.paged.dss.SequentialDisjointSetStruct;

public abstract class WCC<ME extends WCC<ME>> extends Algorithm<ME> {

    protected Graph graph;

    protected final WCC.Config algoConfig;

    public static double defaultWeight(double threshold) {
        return threshold + 1;
    }

    static MemoryEstimation memoryEstimation(
            final boolean incremental,
            Class<? extends WCC<?>> unionFindClass,
            Class<?> taskClass) {
        return MemoryEstimations.builder(unionFindClass)
                .startField("computeStep", taskClass)
                .add(MemoryEstimations.of("DisjointSetStruct", (dimensions, concurrency) -> {
                    MemoryEstimation dssEstimation = (incremental) ?
                            IncrementalDisjointSetStruct.memoryEstimation() :
                            NonInrementalDisjointSetStruct.memoryEstimation();
                    return dssEstimation
                            .estimate(dimensions, concurrency)
                            .memoryUsage()
                            .times(concurrency);
                }))
                .endField()
                .build();
    }

    protected WCC(final Graph graph, final WCC.Config algoConfig) {
        this.graph = graph;
        this.algoConfig = algoConfig;
    }

    public double threshold() {
        return algoConfig.threshold;
    }

    SequentialDisjointSetStruct initDisjointSetStruct(long nodeCount, AllocationTracker tracker) {
        return algoConfig.communityMap == null ?
                new NonInrementalDisjointSetStruct(nodeCount, tracker) :
                new IncrementalDisjointSetStruct(nodeCount, algoConfig.communityMap, tracker);
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
    public void release() {
        graph = null;
    }

    public static class Config {

        public final PropertyMapping communityMap;
        public final double threshold;

        public Config(final PropertyMapping communityMap, final double threshold) {
            this.communityMap = communityMap;
            this.threshold = threshold;
        }
    }
}
