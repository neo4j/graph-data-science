/*
 * Copyright (c) 2017-2021 "Neo4j,"
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.neo4j.gds.embeddings.graphsage.Layer;
import org.neo4j.gds.embeddings.graphsage.ModelData;
import org.neo4j.gds.embeddings.graphsage.SingleLabelFeatureFunction;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSage;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.gds.embeddings.graphsage.algo.ImmutableGraphSageTrainConfig;
import org.neo4j.gds.model.storage.ImmutableModelExportConfig;
import org.neo4j.gds.model.storage.ModelExportConfig;
import org.neo4j.gds.model.storage.ModelToFileExporter;
import org.neo4j.graphalgo.api.schema.GraphSchema;
import org.neo4j.graphalgo.core.model.Model;

import java.io.IOException;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PersistedModelTest {

    private static final String MODEL = "model";
    private static final String USER = "user";

    @TempDir
    Path tempDir;

    private GraphSageTrainConfig trainConfig;
    private ModelData modelData;
    private ModelExportConfig exportConfig;
    private Model<ModelData, GraphSageTrainConfig> model;

    @BeforeEach
    void persistModel() throws IOException {
        trainConfig = ImmutableGraphSageTrainConfig.builder()
            .modelName(MODEL)
            .relationshipWeightProperty("weight")
            .degreeAsProperty(true)
            .concurrency(1)
            .build();

        var modelData = ModelData.of(new Layer[]{}, new SingleLabelFeatureFunction());

        model = Model.of(
            USER,
            MODEL,
            GraphSage.MODEL_TYPE,
            GraphSchema.empty(),
            modelData,
            trainConfig
        );

        exportConfig = ImmutableModelExportConfig
            .builder()
            .fileName(MODEL)
            .build();

        ModelToFileExporter.toFile(tempDir, model, exportConfig);
    }

    @Test
    void testLoadingMetaData() throws IOException {
        var persistedModel = new PersistedModel(tempDir, exportConfig);

        assertThat(persistedModel.username()).isEqualTo(model.username());
        assertThat(persistedModel.name()).isEqualTo(model.name());
        assertThat(persistedModel.trainConfig()).isEqualTo(model.trainConfig());
        assertThat(persistedModel.creationTime()).isEqualTo(model.creationTime());
        assertThat(persistedModel.graphSchema()).isEqualTo(model.graphSchema());
        assertThat(persistedModel.algoType()).isEqualTo(model.algoType());
        assertThat(persistedModel.persisted()).isTrue();
        assertThat(persistedModel.loaded()).isFalse();
    }

    @Test
    void testLoadingData() throws IOException {
        var persistedModel = new PersistedModel(tempDir, exportConfig);

        persistedModel.load();

        assertThat(persistedModel.loaded()).isTrue();
        assertThat(persistedModel.data()).isInstanceOf(ModelData.class);
        var loadedModelData = (ModelData) persistedModel.data();
        assertThat(loadedModelData.layers()).isEmpty();
        assertThat(loadedModelData.featureFunction()).isExactlyInstanceOf(SingleLabelFeatureFunction.class);
    }

    @Test
    void testUnLoadingData() throws IOException {
        var persistedModel = new PersistedModel(tempDir, exportConfig);

        persistedModel.load();
        persistedModel.unload();

        assertThat(persistedModel.loaded()).isFalse();
        assertThatThrownBy(persistedModel::data).hasMessage("The model 'model' is currently not loaded.");
    }
}
