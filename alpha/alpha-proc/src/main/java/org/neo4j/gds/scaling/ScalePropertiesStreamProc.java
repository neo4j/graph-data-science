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
package org.neo4j.gds.scaling;

import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.graphalgo.StreamProc;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ScalePropertiesStreamProc extends StreamProc<ScaleProperties, ScaleProperties.Result, ScalePropertiesStreamProc.Result, ScalePropertiesStreamConfig> {

    @Procedure("gds.alpha.scaleProperties.stream")
    @Description("Scale node properties")
    public Stream<Result> stream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return stream(compute(graphNameOrConfig, configuration));
    }

    @Override
    protected NodeProperties nodeProperties(ComputationResult<ScaleProperties, ScaleProperties.Result, ScalePropertiesStreamConfig> computationResult) {
        return ScalePropertiesProc.nodeProperties(computationResult);
    }

    @Override
    protected Map<String, Class<?>> sharedConfigKeys() {
        return Map.of("nodeProperties", Object.class);
    }

    @Override
    protected ScalePropertiesStreamConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return ScalePropertiesStreamConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<ScaleProperties, ScalePropertiesStreamConfig> algorithmFactory() {
        return new ScalePropertiesFactory<>();
    }

    @Override
    protected Result streamResult(long originalNodeId, long internalNodeId, NodeProperties nodeProperties) {
        return new Result(originalNodeId, nodeProperties.doubleArrayValue(internalNodeId));
    }

    public static class Result {
        public final long nodeId;
        public final List<Double> scaledProperty;

        public Result(long nodeId, double[] scaledProperty) {
            this.nodeId = nodeId;
            this.scaledProperty = Arrays.stream(scaledProperty).boxed().collect(Collectors.toList());
        }
    }
}
