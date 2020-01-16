/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.triangle.TriangleConfig;
import org.neo4j.graphalgo.impl.triangle.TriangleStream;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class TriangleProc extends TriangleBaseProc<TriangleStream, Stream<TriangleStream.Result>, TriangleConfig> {

    @Procedure(name = "gds.alpha.triangle.stream", mode = READ)
    @Description(DESCRIPTION)
    public Stream<TriangleStream.Result> stream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<TriangleStream, Stream<TriangleStream.Result>, TriangleConfig> computationResult =
            compute(graphNameOrConfig, configuration, false, false);

        Graph graph = computationResult.graph();

        if (graph.isEmpty()) {
            graph.release();
            return Stream.empty();
        }

        return computationResult.result();
    }

    @Override
    protected TriangleConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return TriangleConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<TriangleStream, TriangleConfig> algorithmFactory(TriangleConfig config) {
       return new AlphaAlgorithmFactory<TriangleStream, TriangleConfig>() {
           @Override
           public TriangleStream build(
               Graph graph, TriangleConfig configuration, AllocationTracker tracker, Log log
           ) {
               return new TriangleStream(graph, Pools.DEFAULT, configuration.concurrency())
                   .withProgressLogger(ProgressLogger.wrap(log, "Triangle"))
                   .withTerminationFlag(TerminationFlag.wrap(transaction));
           }
       };
    }
}
