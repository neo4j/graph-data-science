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
package org.neo4j.gds.beta.pregel.cc;

import java.util.Map;
import java.util.stream.Stream;
import javax.annotation.processing.Generated;
import org.neo4j.gds.BaseProc;
import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.beta.pregel.PregelProcedureConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.ExecutionMode;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.pregel.proc.PregelStreamProc;
import org.neo4j.gds.pregel.proc.PregelStreamResult;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

@GdsCallable(
    name = "gds.pregel.test.stream",
    executionMode = ExecutionMode.STREAM,
    description = "Test computation description"
)
@Generated("org.neo4j.gds.beta.pregel.PregelProcessor")
public final class ComputationStreamProc extends PregelStreamProc<ComputationAlgorithm, PregelProcedureConfig> {
    @Procedure(
            name = "gds.pregel.test.stream",
            mode = Mode.READ
    )
    @Description("Test computation description")
    public Stream<PregelStreamResult> stream(@Name("graphName") String graphName,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) {
        return stream(compute(graphName, configuration));
    }

    @Procedure(
            name = "gds.pregel.test.stream.estimate",
            mode = Mode.READ
    )
    @Description(BaseProc.ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
            @Name("graphNameOrConfiguration") Object graphNameOrConfiguration,
            @Name("algoConfiguration") Map<String, Object> algoConfiguration) {
        return computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    protected PregelStreamResult streamResult(long originalNodeId, long internalNodeId,
            NodePropertyValues nodePropertyValues
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected PregelProcedureConfig newConfig(String username, CypherMapWrapper config) {
        return PregelProcedureConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<ComputationAlgorithm, PregelProcedureConfig> algorithmFactory(ExecutionContext executionContext) {
        return new ComputationAlgorithmFactory();
    }
}
