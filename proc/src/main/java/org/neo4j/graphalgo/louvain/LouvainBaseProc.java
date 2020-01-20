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
package org.neo4j.graphalgo.louvain;

import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.api.Graph;

import java.util.Collections;
import java.util.stream.Stream;

abstract class LouvainBaseProc<CONFIG extends LouvainBaseConfig> extends AlgoBaseProc<Louvain, Louvain, CONFIG> {

    static final String LOUVAIN_DESCRIPTION =
        "The Louvain method for community detection is an algorithm for detecting communities in networks.";

    @Override
    protected final LouvainFactory<CONFIG> algorithmFactory(CONFIG config) {
        return new LouvainFactory<>();
    }

    protected Stream<LouvainWriteProc.WriteResult> write(
        ComputationResult<Louvain, Louvain, CONFIG> computeResult
    ) {
        CONFIG config = computeResult.config();
        boolean write = config instanceof LouvainWriteConfig;
        LouvainWriteConfig writeConfig = ImmutableLouvainWriteConfig.builder()
            .writeProperty("stats does not support a write property")
            .from(config)
            .build();
        if (computeResult.isGraphEmpty()) {
            return Stream.of(
                new LouvainWriteProc.WriteResult(
                    writeConfig,
                    0, computeResult.createMillis(),
                    0, 0, 0, 0, 0, 0,
                    new double[0], Collections.emptyMap()
                )
            );
        }

        Graph graph = computeResult.graph();
        Louvain louvain = computeResult.algorithm();

        LouvainWriteProc.WriteResultBuilder builder = new LouvainWriteProc.WriteResultBuilder(
            writeConfig,
            graph.nodeCount(),
            callContext,
            computeResult.tracker()
        );

        builder.withCreateMillis(computeResult.createMillis());
        builder.withComputeMillis(computeResult.computeMillis());
        builder
            .withLevels(louvain.levels())
            .withModularity(louvain.modularities()[louvain.levels() - 1])
            .withModularities(louvain.modularities())
            .withCommunityFunction(louvain::getCommunity);

        if (write && !writeConfig.writeProperty().isEmpty()) {
            writeNodeProperties(builder, computeResult);
            graph.releaseProperties();
        }

        return Stream.of(builder.build());
    }
}
