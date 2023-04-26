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

import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageBaseConfig;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageModelResolver;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.gds.executor.validation.AfterLoadValidation;
import org.neo4j.gds.executor.validation.ValidationConfiguration;

import java.util.List;

class GraphSageConfigurationValidation<CONFIG extends GraphSageBaseConfig> implements ValidationConfiguration<CONFIG> {
    private final ModelCatalog catalog;

    GraphSageConfigurationValidation(ModelCatalog catalog) {this.catalog = catalog;}

    @Override
    public List<AfterLoadValidation<CONFIG>> afterLoadValidations() {
        return List.of(
            (graphStore, graphProjectConfig, graphSageConfig) -> {
                Model<ModelData, GraphSageTrainConfig, GraphSageModelTrainer.GraphSageTrainMetrics> model = GraphSageModelResolver.resolveModel(
                    catalog,
                    graphSageConfig.username(),
                    graphSageConfig.modelName()
                );
                GraphSageTrainConfig trainConfig = model.trainConfig();
                trainConfig.graphStoreValidation(
                    graphStore,
                    graphSageConfig.nodeLabelIdentifiers(graphStore),
                    graphSageConfig.internalRelationshipTypes(graphStore)
                );
            }
        );
    }
}
