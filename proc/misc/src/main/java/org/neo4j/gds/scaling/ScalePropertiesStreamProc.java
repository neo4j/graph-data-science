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

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.StreamProc;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STREAM;
import static org.neo4j.gds.scaling.ScalePropertiesProc.SCALE_PROPERTIES_DESCRIPTION;

@GdsCallable(name = "gds.alpha.scaleProperties.stream", description = SCALE_PROPERTIES_DESCRIPTION, executionMode = STREAM)
public class ScalePropertiesStreamProc extends StreamProc<ScaleProperties, ScaleProperties.Result, ScalePropertiesStreamProc.Result, ScalePropertiesStreamConfig> {

    @Procedure("gds.alpha.scaleProperties.stream")
    @Description(SCALE_PROPERTIES_DESCRIPTION)
    public Stream<Result> stream(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return stream(compute(graphName, configuration));
    }

    @Override
    protected NodePropertyValues nodeProperties(ComputationResult<ScaleProperties, ScaleProperties.Result, ScalePropertiesStreamConfig> computationResult) {
        return ScalePropertiesProc.nodeProperties(computationResult);
    }

    @Override
    protected ScalePropertiesStreamConfig newConfig(String username, CypherMapWrapper config) {
        return ScalePropertiesStreamConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<ScaleProperties, ScalePropertiesStreamConfig> algorithmFactory() {
        return new ScalePropertiesFactory<>();
    }

    @Override
    protected Result streamResult(long originalNodeId, long internalNodeId, NodePropertyValues nodePropertyValues) {
        return new Result(originalNodeId, nodePropertyValues.doubleArrayValue(internalNodeId));
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
