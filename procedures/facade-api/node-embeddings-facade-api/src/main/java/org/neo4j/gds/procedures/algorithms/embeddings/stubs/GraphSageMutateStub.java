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
package org.neo4j.gds.procedures.algorithms.embeddings.stubs;

import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageMutateConfig;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.procedures.algorithms.embeddings.DefaultNodeEmbeddingMutateResult;
import org.neo4j.gds.procedures.algorithms.stubs.MutateStub;

import java.util.Map;
import java.util.stream.Stream;

public interface GraphSageMutateStub extends MutateStub<GraphSageMutateConfig, DefaultNodeEmbeddingMutateResult> {
    @Override
    GraphSageMutateConfig parseConfiguration(Map<String, Object> configuration);

    @Override
    MemoryEstimation getMemoryEstimation(String username, Map<String, Object> rawConfiguration);

    @Override
    Stream<MemoryEstimateResult> estimate(Object graphNameAsString, Map<String, Object> rawConfiguration);

    @Override
    Stream<DefaultNodeEmbeddingMutateResult> execute(
        String graphNameAsString,
        Map<String, Object> rawConfiguration
    );
}
