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
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.triangle.ModernTriangleStream;
import org.neo4j.graphalgo.impl.triangle.TriangleConfig;
import org.neo4j.graphalgo.impl.triangle.TriangleCountBase;
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

public class ModernTriangleProc extends AlgoBaseProc<ModernTriangleStream, Stream<ModernTriangleStream.Result>, TriangleConfig> {

    @Procedure(name = "gds.alpha.triangle.stream", mode = READ)
    @Description("Triangle counting is a community detection graph algorithm that is used to determine the number of triangles passing through each node in the graph.")
    public Stream<ModernTriangleStream.Result> stream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {

        ComputationResult<ModernTriangleStream, Stream<ModernTriangleStream.Result>, TriangleConfig> computationResult =
            compute(graphNameOrConfig, configuration);

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
    protected AlgorithmFactory<ModernTriangleStream, TriangleConfig> algorithmFactory(TriangleConfig config) {
       return new AlgorithmFactory<ModernTriangleStream, TriangleConfig>() {
           @Override
           public ModernTriangleStream build(
               Graph graph, TriangleConfig configuration, AllocationTracker tracker, Log log
           ) {
               return new ModernTriangleStream(graph, Pools.DEFAULT, configuration.concurrency())
                   .withProgressLogger(ProgressLogger.wrap(log, "Triangle"))
                   .withTerminationFlag(TerminationFlag.wrap(transaction));
           }

           @Override
           public MemoryEstimation memoryEstimation(TriangleConfig configuration) {
               return MemoryEstimations.empty();
           }
       };
    }
}
