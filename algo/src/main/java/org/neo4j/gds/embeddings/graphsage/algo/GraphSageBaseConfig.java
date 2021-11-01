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
package org.neo4j.gds.embeddings.graphsage.algo;

import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.BatchSizeConfig;
import org.neo4j.gds.config.RelationshipWeightConfig;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.embeddings.graphsage.GraphSageModelTrainer;
import org.neo4j.gds.embeddings.graphsage.ModelData;
import org.neo4j.gds.model.ModelConfig;

public interface GraphSageBaseConfig extends AlgoBaseConfig, BatchSizeConfig, ModelConfig, RelationshipWeightConfig {
    long serialVersionUID = 0x42L;

    @Value.Derived
    @Configuration.Ignore
    default Model<ModelData, GraphSageTrainConfig, GraphSageModelTrainer.GraphSageTrainMetrics> model() {
        // Need to resolve the model at config-level to reuse the relationship-property
        return ModelCatalog.get(username(), modelName(), ModelData.class, GraphSageTrainConfig.class, GraphSageModelTrainer.GraphSageTrainMetrics.class);
    }

    @Override
    @Value.Derived
    @Configuration.Ignore
    default @Nullable String relationshipWeightProperty() {
        return model().trainConfig().relationshipWeightProperty();
    }
}
