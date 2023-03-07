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
import org.neo4j.gds.WriteProc;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.ExecutionMode;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.StandardWriteResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.scaling.ScalePropertiesProc.SCALE_PROPERTIES_DESCRIPTION;
import static org.neo4j.procedure.Mode.WRITE;

@GdsCallable(name = "gds.beta.scaleProperties.write", description = SCALE_PROPERTIES_DESCRIPTION, executionMode = ExecutionMode.WRITE_NODE_PROPERTY)
public class ScalePropertiesWriteProc extends WriteProc<ScaleProperties, ScaleProperties.Result, ScalePropertiesWriteProc.WriteResult, ScalePropertiesWriteConfig> {

    @Procedure(value = "gds.beta.scaleProperties.write", mode = WRITE)
    @Description(SCALE_PROPERTIES_DESCRIPTION)
    public Stream<ScalePropertiesWriteProc.WriteResult> write(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<ScaleProperties, ScaleProperties.Result, ScalePropertiesWriteConfig> computationResult = compute(
            graphName,
            configuration
        );
        return write(computationResult);
    }

    @Override
    protected ScalePropertiesWriteConfig newConfig(String username, CypherMapWrapper config) {
        return ScalePropertiesWriteConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<ScaleProperties, ScalePropertiesWriteConfig> algorithmFactory() {
        return new ScalePropertiesFactory<>();
    }

    @Override
    protected NodePropertyValues nodeProperties(
        ComputationResult<ScaleProperties, ScaleProperties.Result, ScalePropertiesWriteConfig> computationResult
    ) {
        return ScalePropertiesProc.nodeProperties(computationResult);
    }

    @Override
    protected AbstractResultBuilder<WriteResult> resultBuilder(
        ComputationResult<ScaleProperties, ScaleProperties.Result, ScalePropertiesWriteConfig> computeResult,
        ExecutionContext executionContext
    ) {
        return new WriteResult.Builder().withScalerStatistics(computeResult.result().scalerStatistics());
    }

    @SuppressWarnings("unused")
    public static final class WriteResult extends StandardWriteResult {

        public final long nodePropertiesWritten;
        public final Map<String, Map<String, List<Double>>> scalerStatistics;

        WriteResult(
            Map<String, Map<String, List<Double>>> scalerStatistics,
            long preProcessingMillis,
            long computeMillis,
            long writeMillis,
            long nodePropertiesWritten,
            Map<String, Object> configuration
        ) {
            super(
                preProcessingMillis,
                computeMillis,
                0L,
                writeMillis,
                configuration
            );
            this.nodePropertiesWritten = nodePropertiesWritten;
            this.scalerStatistics = scalerStatistics;
        }

        static class Builder extends AbstractResultBuilder<WriteResult> {

            private Map<String, Map<String, List<Double>>> scalerStatistics;

            Builder withScalerStatistics(Map<String, Map<String, List<Double>>> stats) {
                this.scalerStatistics = stats;
                return this;
            }

            @Override
            public WriteResult build() {
                return new WriteResult(
                    scalerStatistics,
                    preProcessingMillis,
                    computeMillis,
                    writeMillis,
                    nodePropertiesWritten,
                    config.toMap()
                );
            }
        }

    }
}
