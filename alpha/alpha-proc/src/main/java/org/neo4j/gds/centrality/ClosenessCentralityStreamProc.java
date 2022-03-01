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

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.StreamProc;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.validation.ValidationConfiguration;
import org.neo4j.gds.impl.closeness.ClosenessCentralityStreamConfig;
import org.neo4j.gds.impl.closeness.ClosenessCentrality;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.centrality.ClosenessCentralityProc.DESCRIPTION;
import static org.neo4j.gds.executor.ExecutionMode.STREAM;
import static org.neo4j.procedure.Mode.READ;

@GdsCallable(name = "gds.alpha.closeness.stream", description = DESCRIPTION, executionMode = STREAM)
public class ClosenessCentralityStreamProc extends StreamProc<ClosenessCentrality, ClosenessCentrality, ClosenessCentrality.Result, ClosenessCentralityStreamConfig> {

    @Override
    public String name() {
        return "ClosenessCentrality";
    }

    @Procedure(value = "gds.alpha.closeness.stream", mode = READ)
    @Description(DESCRIPTION)
    public Stream<ClosenessCentrality.Result> stream(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var computationResult = compute(
            graphName,
            configuration
        );
        return stream(computationResult);
    }

    @Override
    protected ClosenessCentrality.Result streamResult(
        long originalNodeId, long internalNodeId, NodeProperties nodeProperties
    ) {
        return new ClosenessCentrality.Result(originalNodeId, nodeProperties.doubleValue(internalNodeId));
    }

    @Override
    protected NodeProperties nodeProperties(ComputationResult<ClosenessCentrality, ClosenessCentrality, ClosenessCentralityStreamConfig> computationResult) {
        return ClosenessCentralityProc.nodeProperties(computationResult);
    }

    @Override
    protected ClosenessCentralityStreamConfig newConfig(String username, CypherMapWrapper config) {
        return ClosenessCentralityStreamConfig.of(config);
    }

    @Override
    public ValidationConfiguration<ClosenessCentralityStreamConfig> validationConfig() {
        return ClosenessCentralityProc.getValidationConfig();
    }

    @Override
    public GraphAlgorithmFactory<ClosenessCentrality, ClosenessCentralityStreamConfig> algorithmFactory() {
        return ClosenessCentralityProc.algorithmFactory();
    }

}
