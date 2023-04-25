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
package org.neo4j.gds.wcc;

import org.neo4j.gds.CommunityProcCompanion;
import org.neo4j.gds.WriteNodePropertiesComputationResultConsumer;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.api.properties.nodes.EmptyLongNodePropertyValues;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.core.write.NodeProperty;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.ExecutionMode;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.result.AbstractCommunityResultBuilder;
import org.neo4j.gds.result.AbstractResultBuilder;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@GdsCallable(name = "gds.wcc.write", description = WccSpecification.WCC_DESCRIPTION, executionMode = ExecutionMode.WRITE_NODE_PROPERTY)
public class WccWriteSpecification implements AlgorithmSpec<Wcc, DisjointSetStruct, WccWriteConfig, Stream<WccWriteSpecification.WriteResult>, WccAlgorithmFactory<WccWriteConfig>> {

    @Override
    public String name() {
        return "WccWrite";
    }

    @Override
    public WccAlgorithmFactory<WccWriteConfig> algorithmFactory() {
        return new WccAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<WccWriteConfig> newConfigFunction() {
        return (__, userInput) -> WccWriteConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<Wcc, DisjointSetStruct, WccWriteConfig, Stream<WriteResult>> computationResultConsumer() {
        return new WriteNodePropertiesComputationResultConsumer<>(
            this::resultBuilder,
            this::nodeProperties,
            name()
        );
    }

    private AbstractResultBuilder<WriteResult> resultBuilder(
        ComputationResult<Wcc, DisjointSetStruct, WccWriteConfig> computeResult,
        ExecutionContext executionContext
    ) {
        return WccSpecification.resultBuilder(
            new WriteResult.Builder(executionContext.returnColumns(), computeResult.config().concurrency()),
            computeResult
        );
    }

    private List<NodeProperty> nodeProperties(ComputationResult<Wcc, DisjointSetStruct, WccWriteConfig> computationResult) {
        var config = computationResult.config();
        return List.of(
            NodeProperty.of(
                config.writeProperty(),
                CommunityProcCompanion.nodeProperties(
                    config,
                    config.writeProperty(),
                    computationResult.result()
                        .map(DisjointSetStruct::asNodeProperties)
                        .orElse(EmptyLongNodePropertyValues.INSTANCE),
                    () -> computationResult.graphStore().nodeProperty(config.seedProperty())
                )
            )
        );
    }

    @SuppressWarnings("unused")
    public static final class WriteResult extends WccStatsSpecification.StatsResult {

        public final long writeMillis;
        public final long nodePropertiesWritten;

        WriteResult(
            long componentCount,
            Map<String, Object> componentDistribution,
            long preProcessingMillis,
            long computeMillis,
            long postProcessingMillis,
            long writeMillis,
            long nodePropertiesWritten,
            Map<String, Object> configuration
        ) {
            super(
                componentCount,
                componentDistribution,
                preProcessingMillis,
                computeMillis,
                postProcessingMillis,
                configuration
            );
            this.writeMillis = writeMillis;
            this.nodePropertiesWritten = nodePropertiesWritten;
        }

        static class Builder extends AbstractCommunityResultBuilder<WriteResult> {

            Builder(ProcedureReturnColumns returnColumns, int concurrency) {
                super(returnColumns, concurrency);
            }

            @Override
            protected WriteResult buildResult() {
                return new WriteResult(
                    maybeCommunityCount.orElse(0L),
                    communityHistogramOrNull(),
                    preProcessingMillis,
                    computeMillis,
                    postProcessingDuration,
                    writeMillis,
                    nodePropertiesWritten,
                    config.toMap()
                );
            }
        }
    }
}
