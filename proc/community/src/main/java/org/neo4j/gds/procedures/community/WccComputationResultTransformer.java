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
package org.neo4j.gds.procedures.community;

import org.neo4j.gds.CommunityProcCompanion;
import org.neo4j.gds.GraphStoreUpdater;
import org.neo4j.gds.algorithms.ComputationResult;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.api.properties.nodes.EmptyLongNodePropertyValues;
import org.neo4j.gds.config.MutateNodePropertyConfig;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.core.write.NodeProperty;
import org.neo4j.gds.result.AbstractCommunityResultBuilder;
import org.neo4j.gds.wcc.WccBaseConfig;
import org.neo4j.gds.wcc.WccMutateResult;

import java.util.List;
import java.util.stream.LongStream;
import java.util.stream.Stream;

final class WccComputationResultTransformer {

    private WccComputationResultTransformer() {}

    static Stream<WccStreamResult> toStreamResult(ComputationResult<WccBaseConfig, DisjointSetStruct> computationResult) {
        return computationResult.result().map(wccResult -> {
            var graph = computationResult.graph();

            var nodePropertyValues = CommunityProcCompanion.nodeProperties(
                computationResult.config(),
                wccResult.asNodeProperties()
            );

            return LongStream
                .range(IdMap.START_NODE_ID, graph.nodeCount())
                .filter(nodePropertyValues::hasValue)
                .mapToObj(nodeId -> new WccStreamResult(
                    graph.toOriginalNodeId(nodeId),
                    nodePropertyValues.longValue(nodeId)
                ));

        }).orElseGet(Stream::empty);
    }

    // TODO: This might be shared between the clients. Think of a better placement for this transformation...
    static WccMutateResult toMutateResult(
        ComputationResult<WccBaseConfig, DisjointSetStruct> computationResult,
        MutateNodePropertyConfig mutateConfig
    ) {

        var config = computationResult.config();

        var nodePropertyValues = CommunityProcCompanion.nodeProperties(
            config,
            mutateConfig.mutateProperty(),
            computationResult.result()
                .map(DisjointSetStruct::asNodeProperties)
                .orElse(EmptyLongNodePropertyValues.INSTANCE),
            () -> computationResult.graphStore().nodeProperty(config.seedProperty())
        );

        var nodeProperties = List.of(
            NodeProperty.of(
                mutateConfig.mutateProperty(),
                nodePropertyValues
            ));

        var resultBuilder = resultBuilder(computationResult);

        // Go and mutate the graph store...
        GraphStoreUpdater.updateGraphStore(
            computationResult.graph(),
            computationResult.graphStore(),
            resultBuilder,
            mutateConfig,
            null,
            nodeProperties
        );

        return resultBuilder.build();
    }

    private static AbstractCommunityResultBuilder<WccMutateResult> resultBuilder(
        ComputationResult<WccBaseConfig, DisjointSetStruct> computationResult
    ) {
        var mutateREsultBuilder = new WccMutateResult.Builder(
            ProcedureReturnColumns.EMPTY,
            computationResult.config().concurrency()
        );

        computationResult.result().ifPresent(result -> mutateREsultBuilder.withCommunityFunction(result::setIdOf));

        return mutateREsultBuilder;
    }
}
