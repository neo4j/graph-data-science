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
package org.neo4j.gds.centrality;

import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.impl.closeness.HarmonicCentralityConfig;
import org.neo4j.gds.impl.harmonic.HarmonicCentrality;
import org.neo4j.gds.impl.harmonic.HarmonicResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.gds.centrality.HarmonicCentralityProc.DESCRIPTION;
import static org.neo4j.gds.executor.ExecutionMode.STREAM;
import static org.neo4j.procedure.Mode.READ;

@GdsCallable(name = "gds.alpha.closeness.harmonic.stream", description = DESCRIPTION, executionMode = STREAM)
public class HarmonicCentralityStreamProc extends HarmonicCentralityProc<HarmonicCentralityStreamProc.StreamResult> {

    @Procedure(name = "gds.alpha.closeness.harmonic.stream", mode = READ)
    @Description(DESCRIPTION)
    public Stream<StreamResult> stream(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var computationResult = compute(graphName, configuration);
        return computationResultConsumer().consume(computationResult, executionContext());
    }

    @Override
    public ComputationResultConsumer<HarmonicCentrality, HarmonicResult, HarmonicCentralityConfig, Stream<StreamResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> {
            var result = computationResult.result();

            if (result.isEmpty()) {
                return Stream.empty();
            }

            var graph = computationResult.graph();
            var centralityScores = result.get();
            return LongStream.range(0, graph.nodeCount())
                .boxed()
                .map(nodeId -> new StreamResult(graph.toOriginalNodeId(nodeId), centralityScores.getCentralityScore(nodeId)));
        };
    }

    @SuppressWarnings("unused")
    public static final class StreamResult {
        public final long nodeId;
        public final double centrality;

        StreamResult(long nodeId, double centrality) {
            this.nodeId = nodeId;
            this.centrality = centrality;
        }
    }
}
