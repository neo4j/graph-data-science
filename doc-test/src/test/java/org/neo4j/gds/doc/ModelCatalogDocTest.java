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
package org.neo4j.gds.doc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.neo4j.gds.api.schema.MutableGraphSchema;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.embeddings.graphsage.GraphSageModelTrainer;
import org.neo4j.gds.embeddings.graphsage.Layer;
import org.neo4j.gds.embeddings.graphsage.ModelData;
import org.neo4j.gds.embeddings.graphsage.SingleLabelFeatureFunction;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSage;
import org.neo4j.gds.embeddings.graphsage.algo.ImmutableGraphSageTrainConfig;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jModelCatalogExtension;

@Neo4jModelCatalogExtension
abstract class ModelCatalogDocTest extends SingleFileDocTestBase {

    @Inject
    ModelCatalog modelCatalog;

    @BeforeEach
    void loadModel() {
        modelCatalog.set(Model.of(
            GraphSage.MODEL_TYPE,
            MutableGraphSchema.empty(),
            ModelData.of(new Layer[0], new SingleLabelFeatureFunction()),
            ImmutableGraphSageTrainConfig
                .builder()
                .modelUser(getUsername())
                .modelName("my-model")
                .addFeatureProperties("a")
                .build(),
            GraphSageModelTrainer.GraphSageTrainMetrics.empty()
        ));
    }

    @AfterEach
    void afterAll() {
        modelCatalog.removeAllLoadedModels();
    }
}
