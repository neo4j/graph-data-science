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
package org.neo4j.gds.ml.splitting;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

public class SplitRelationships extends Algorithm<EdgeSplitter.SplitResult> {

    private final Graph graph;
    private final Graph masterGraph;
    private final SplitRelationshipsMutateConfig config;

    public SplitRelationships(Graph graph, Graph masterGraph, SplitRelationshipsMutateConfig config) {
        super(ProgressTracker.NULL_TRACKER);
        this.graph = graph;
        this.masterGraph = masterGraph;
        this.config = config;
    }

    static MemoryEstimation estimate(SplitRelationshipsMutateConfig configuration) {
        // we cannot assume any compression of the relationships
        var pessimisticSizePerRel = configuration.hasRelationshipWeightProperty()
            ? Double.BYTES + 2 * Long.BYTES
            : 2 * Long.BYTES;

        return MemoryEstimations.builder("Relationship splitter")
            .perGraphDimension("Selected relationships", (graphDimensions, threads) -> {
                var positiveRelCount = graphDimensions.estimatedRelCount(configuration.relationshipTypes()) * configuration.holdoutFraction();
                var negativeRelCount = positiveRelCount * configuration.negativeSamplingRatio();
                long selectedRelCount = (long) (positiveRelCount + negativeRelCount);

                // Whether the graph is undirected or directed
                return MemoryRange.of(selectedRelCount / 2, selectedRelCount).times(pessimisticSizePerRel);
            })
            .perGraphDimension("Remaining relationships", (graphDimensions, threads) -> {
                long remainingRelCount = (long) (graphDimensions.estimatedRelCount(configuration.relationshipTypes()) * (1 - configuration.holdoutFraction()));
                // remaining relationships are always undirected
                return MemoryRange.of(remainingRelCount * pessimisticSizePerRel);
            })
            .build();
    }

    @Override
    public EdgeSplitter.SplitResult compute() {
        var splitter = graph.schema().isUndirected()
            ? new UndirectedEdgeSplitter(config.randomSeed(), config.negativeSamplingRatio())
            : new DirectedEdgeSplitter(config.randomSeed(), config.negativeSamplingRatio());
        return splitter.split(graph, masterGraph, config.holdoutFraction());
    }

    @Override
    public void release() {

    }
}
