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
package org.neo4j.gds.triangle;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.WriteProc;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.validation.ValidationConfiguration;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.WRITE_NODE_PROPERTY;
import static org.neo4j.gds.triangle.LocalClusteringCoefficientCompanion.DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

@GdsCallable(name = "gds.localClusteringCoefficient.write", description = DESCRIPTION, executionMode = WRITE_NODE_PROPERTY)
public class LocalClusteringCoefficientWriteProc extends WriteProc<LocalClusteringCoefficient, LocalClusteringCoefficient.Result, LocalClusteringCoefficientWriteProc.WriteResult, LocalClusteringCoefficientWriteConfig> {

    @Procedure(value = "gds.localClusteringCoefficient.write", mode = WRITE)
    @Description(DESCRIPTION)
    public Stream<WriteResult> write(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return write(compute(graphName, configuration));
    }

    @Procedure(value = "gds.localClusteringCoefficient.write.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        return computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    protected LocalClusteringCoefficientWriteConfig newConfig(String username, CypherMapWrapper config) {
        return LocalClusteringCoefficientWriteConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<LocalClusteringCoefficient, LocalClusteringCoefficientWriteConfig> algorithmFactory() {
        return new LocalClusteringCoefficientFactory<>();
    }

    @Override
    public ValidationConfiguration<LocalClusteringCoefficientWriteConfig> validationConfig(ExecutionContext executionContext) {
        return LocalClusteringCoefficientCompanion.getValidationConfig(executionContext.log());
    }

    @Override
    protected NodePropertyValues nodeProperties(
        ComputationResult<LocalClusteringCoefficient, LocalClusteringCoefficient.Result, LocalClusteringCoefficientWriteConfig> computationResult
    ) {
        return LocalClusteringCoefficientCompanion.nodeProperties(computationResult);
    }

    @Override
    protected AbstractResultBuilder<WriteResult> resultBuilder(
        ComputationResult<LocalClusteringCoefficient, LocalClusteringCoefficient.Result, LocalClusteringCoefficientWriteConfig> computeResult,
        ExecutionContext executionContext
    ) {
        return LocalClusteringCoefficientCompanion.resultBuilder(
            new LocalClusteringCoefficientWriteResultBuilder(
                executionContext.returnColumns(),
                computeResult.config().concurrency()
            ),
            computeResult
        );
    }

    @SuppressWarnings("unused")
    public static class WriteResult extends LocalClusteringCoefficientStatsProc.StatsResult {

        public long writeMillis;
        public long nodePropertiesWritten;

        WriteResult(
            double averageClusteringCoefficient,
            long nodeCount,
            long preProcessingMillis,
            long computeMillis,
            long writeMillis,
            long nodePropertiesWritten,
            Map<String, Object> configuration
        ) {
            super(averageClusteringCoefficient, nodeCount, preProcessingMillis, computeMillis, configuration);
            this.nodePropertiesWritten = nodePropertiesWritten;
            this.writeMillis = writeMillis;
        }
    }


    static class LocalClusteringCoefficientWriteResultBuilder extends LocalClusteringCoefficientCompanion.ResultBuilder<WriteResult> {

        LocalClusteringCoefficientWriteResultBuilder(
            ProcedureReturnColumns returnColumns,
            int concurrency
        ) {
            super(returnColumns, concurrency);
        }

        @Override
        protected WriteResult buildResult() {
            return new WriteResult(
                averageClusteringCoefficient,
                nodeCount,
                preProcessingMillis,
                computeMillis,
                writeMillis,
                nodePropertiesWritten,
                config.toMap()
            );
        }
    }
}
