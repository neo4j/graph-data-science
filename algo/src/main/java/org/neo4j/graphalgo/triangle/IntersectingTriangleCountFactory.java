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
package org.neo4j.graphalgo.triangle;

import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.api.FilterGraph;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.huge.CompositeAdjacencyList;
import org.neo4j.graphalgo.core.huge.FilteredGraphIntersect;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.huge.HugeGraphIntersect;
import org.neo4j.graphalgo.core.huge.NodeFilteredGraph;
import org.neo4j.graphalgo.core.huge.UnionGraph;
import org.neo4j.graphalgo.core.huge.UnionGraphIntersect;
import org.neo4j.graphalgo.core.intersect.RelationshipIntersectFactoryLocator;
import org.neo4j.graphalgo.core.utils.BatchingProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.paged.HugeAtomicLongArray;
import org.neo4j.graphalgo.core.utils.progress.ProgressEventTracker;
import org.neo4j.logging.Log;

import static org.neo4j.graphalgo.core.intersect.RelationshipIntersectFactoryLocator.register;

public class IntersectingTriangleCountFactory<CONFIG extends TriangleCountBaseConfig> implements AlgorithmFactory<IntersectingTriangleCount, CONFIG> {

    static {
        register(HugeGraph.class, (graph, config) -> {
            var topology = graph.relationshipTopology();
            return new HugeGraphIntersect(topology.list(), topology.offsets(), config.maxDegree());
        });

        register(UnionGraph.class, (graph, config) -> new UnionGraphIntersect(
            (CompositeAdjacencyList) graph.relationshipTopology().list(),
            config.maxDegree()
        ));

        register(FilterGraph.class, (graph, config) -> {
            var innerGraph = graph.graph();
            return RelationshipIntersectFactoryLocator.lookup(innerGraph.getClass())
                .orElseThrow(UnsupportedOperationException::new)
                .create(innerGraph, config);
        });

        register(NodeFilteredGraph.class, (graph, config) -> {
            var innerGraph = graph.graph();
            var relationshipIntersect = RelationshipIntersectFactoryLocator.lookup(innerGraph.getClass())
                .orElseThrow(UnsupportedOperationException::new)
                .create(innerGraph, config);
            return new FilteredGraphIntersect(graph.nodeMapping(), relationshipIntersect);
        });
    }

    @Override
    public IntersectingTriangleCount build(
        Graph graph, CONFIG configuration, AllocationTracker tracker, Log log,
        ProgressEventTracker eventTracker
    ) {

        ProgressLogger progressLogger = new BatchingProgressLogger(
            log,
            graph.nodeCount(),
            getClass().getSimpleName(),
            configuration.concurrency(),
            eventTracker
        );

        return IntersectingTriangleCount.create(
            graph,
            configuration,
            Pools.DEFAULT,
            tracker,
            progressLogger
        );
    }

    @Override
    public MemoryEstimation memoryEstimation(CONFIG configuration) {
        return MemoryEstimations
            .builder(IntersectingTriangleCount.class)
            .perNode("triangle-counts", HugeAtomicLongArray::memoryEstimation)
            .build();
    }
}
