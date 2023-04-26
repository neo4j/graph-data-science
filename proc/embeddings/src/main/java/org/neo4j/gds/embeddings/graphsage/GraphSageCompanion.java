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
package org.neo4j.gds.embeddings.graphsage;

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.api.properties.nodes.DoubleArrayNodePropertyValues;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSage;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageBaseConfig;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageResult;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.validation.ValidationConfiguration;

public final class GraphSageCompanion {

    static final String GRAPHSAGE_DESCRIPTION = "The GraphSage algorithm inductively computes embeddings for nodes based on a their features and neighborhoods.";

    private GraphSageCompanion() {}

    @NotNull
    static <T extends GraphSageBaseConfig> DoubleArrayNodePropertyValues getNodeProperties(ComputationResult<GraphSage, GraphSageResult, T> computationResult) {
        var embeddings = computationResult.result()
            .map(GraphSageResult::embeddings)
            .orElseGet(() -> HugeObjectArray.newArray(double[].class, 0));
        var size = embeddings.size();

        return new DoubleArrayNodePropertyValues() {
            @Override
            public long nodeCount() {
                return size;
            }

            @Override
            public double[] doubleArrayValue(long nodeId) {

                return embeddings.get(nodeId);
            }
        };
    }

    static <CONFIG extends GraphSageBaseConfig> ValidationConfiguration<CONFIG> getValidationConfig(ModelCatalog catalog) {
        return new GraphSageConfigurationValidation<>(catalog);
    }

}
