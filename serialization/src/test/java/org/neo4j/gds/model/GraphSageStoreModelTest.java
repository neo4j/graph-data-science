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
package org.neo4j.gds.model;

import org.neo4j.gds.embeddings.graphsage.EmptyGraphSageTrainMetrics;
import org.neo4j.gds.embeddings.graphsage.Layer;
import org.neo4j.gds.embeddings.graphsage.ModelData;
import org.neo4j.gds.embeddings.graphsage.SingleLabelFeatureFunction;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSage;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.gds.embeddings.graphsage.algo.ImmutableGraphSageTrainConfig;
import org.neo4j.graphalgo.api.schema.GraphSchema;
import org.neo4j.graphalgo.core.model.Model;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

class GraphSageStoreModelTest extends BaseStoreModelTest<ModelData, GraphSageTrainConfig> {

    @Override
    void testLoadingData() throws IOException {
        var storedModel = new StoredModel(tempDir);

        storedModel.load();

        assertThat(storedModel.loaded()).isTrue();
        assertThat(storedModel.data()).isInstanceOf(ModelData.class);
        var loadedModelData = (ModelData) storedModel.data();
        assertThat(loadedModelData.layers()).isEmpty();
        assertThat(loadedModelData.featureFunction()).isExactlyInstanceOf(SingleLabelFeatureFunction.class);
    }

    @Override
    Model<ModelData, GraphSageTrainConfig> model() {
        GraphSageTrainConfig trainConfig = ImmutableGraphSageTrainConfig.builder()
            .modelName(MODEL)
            .addFeatureProperties("a")
            .relationshipWeightProperty("weight")
            .build();

        var modelData = ModelData.of(new Layer[]{}, new SingleLabelFeatureFunction());

        return Model.of(
            USER,
            MODEL,
            GraphSage.MODEL_TYPE,
            GraphSchema.empty(),
            modelData,
            trainConfig,
            EmptyGraphSageTrainMetrics.instance
        );
    }
}
