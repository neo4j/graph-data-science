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
package org.neo4j.gds.procedures.algorithms.pathfinding.write;

import org.neo4j.gds.core.loading.GraphResources;
import org.neo4j.gds.pathfinding.SpanningTreeWriteStep;
import org.neo4j.gds.procedures.algorithms.pathfinding.SpanningTreeWriteResult;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformer;
import org.neo4j.gds.results.ResultTransformerBuilder;
import org.neo4j.gds.spanningtree.SpanningTree;
import org.neo4j.gds.spanningtree.SpanningTreeWriteConfig;

import java.util.stream.Stream;

class SpanningTreeWriteResultTransformerBuilder implements ResultTransformerBuilder<TimedAlgorithmResult<SpanningTree>, Stream<SpanningTreeWriteResult>> {

    private final SpanningTreeWriteConfig config;
    private final SpanningTreeWriteStep writeStep;

    SpanningTreeWriteResultTransformerBuilder(
        SpanningTreeWriteStep writeStep,
        SpanningTreeWriteConfig config
    ) {
        this.writeStep = writeStep;
        this.config = config;
    }

    @Override
    public ResultTransformer<TimedAlgorithmResult<SpanningTree>, Stream<SpanningTreeWriteResult>> build(GraphResources graphResources) {
        return new SpanningTreeWriteResultTransformer(
            writeStep,
            graphResources.graph(),
            graphResources.graphStore(),
            graphResources.resultStore(),
            config.jobId(),
            config.toMap()
        );    }
}
