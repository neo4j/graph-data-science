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

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.PagedDisjointSetStruct;
import org.neo4j.graphdb.Direction;

import java.util.Optional;

/**
 * Sequential UnionFind:
 * <p>
 * The algorithm computes sets of weakly connected components.
 * <p>
 * The impl. is based on the {@link PagedDisjointSetStruct}. It iterates over all relationships once
 * within a single forEach loop and adds each source-target pair to the struct. Therefore buffering
 * would introduce an overhead (a SingleRun-RelationshipIterable might be used here).
 * <p>
 * There are 2 different methods for computing the component-sets. compute() calculates all weakly
 * components regardless of the actual weight of the relationship while compute(threshold:double)
 * on the other hand only takes the transition into account if the weight exceeds the threshold value.
 *
 * @author mknblch
 */
public class GraphUnionFind extends GraphUnionFindAlgo<GraphUnionFind> {

    private static final MemoryEstimation MEMORY_ESTIMATION = MemoryEstimations.builder(GraphUnionFind.class)
            .add("dss", PagedDisjointSetStruct.MEMORY_ESTIMATION)
            .build();

    private PagedDisjointSetStruct dss;
    private final long nodeCount;
    private RelationshipConsumer unrestricted;

    public GraphUnionFind(
            Optional<Graph> graph,
            AllocationTracker tracker,
            double threshold) {
        super(graph.orElse(null), threshold);
        this.threshold = threshold;
        if (graph.isPresent()) {
            this.graph = graph.get();
            this.nodeCount = this.graph.nodeCount();
            this.dss = new PagedDisjointSetStruct(nodeCount, tracker);
            this.unrestricted = (source, target) -> {
                dss.union(source, target);
                return true;
            };
        } else {
            this.graph = null;
            this.nodeCount = 0;
            this.dss = null;
            this.unrestricted = (source, target) -> true;
        }
    }

    @Override
    public MemoryEstimation memoryEstimation() {
        return MEMORY_ESTIMATION;
    }

    /**
     * Compute unions if relationship weight exceeds threshold
     *
     * @param threshold the minimum threshold
     * @return a DSS
     */
    @Override
    public PagedDisjointSetStruct compute(final double threshold) {
        return compute(new WithThreshold(threshold));
    }

    /**
     * Compute unions of connected nodes
     *
     * @return a DSS
     */
    @Override
    public PagedDisjointSetStruct computeUnrestricted() {
        return compute(unrestricted);
    }

    @Override
    public GraphUnionFind release() {
        dss = null;
        unrestricted = null;
        return super.release();
    }

    private PagedDisjointSetStruct compute(RelationshipConsumer consumer) {
        dss.reset();
        final ProgressLogger progressLogger = getProgressLogger();
        graph.forEachNode((long node) -> {
            if (!running()) {
                return false;
            }
            graph.forEachRelationship(node, Direction.OUTGOING, consumer);
            progressLogger.logProgress((double) node / (nodeCount - 1));
            return true;
        });
        return dss;
    }

    private final class WithThreshold implements RelationshipConsumer {
        private final double threshold;

        private WithThreshold(final double threshold) {
            this.threshold = threshold;
        }

        @Override
        public boolean accept(
                final long source,
                final long target) {
            double weight = graph.weightOf(source, target);
            if (weight >= threshold) {
                dss.union(source, target);
            }
            return true;
        }
    }
}
