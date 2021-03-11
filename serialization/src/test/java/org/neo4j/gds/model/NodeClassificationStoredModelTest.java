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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Weights;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Matrix;
import org.neo4j.gds.embeddings.graphsage.subgraph.LocalIdMap;
import org.neo4j.gds.ml.nodemodels.NodeClassificationTrain;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeClassificationTrainConfig;
import org.neo4j.gds.ml.nodemodels.metrics.Metric;
import org.neo4j.gds.ml.nodemodels.multiclasslogisticregression.MultiClassNLRData;
import org.neo4j.gds.model.storage.ModelToFileExporter;
import org.neo4j.graphalgo.api.schema.GraphSchema;
import org.neo4j.graphalgo.core.model.Model;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.model.storage.ModelToFileExporter.META_DATA_FILE;

class NodeClassificationStoredModelTest {

    private static final String MODEL = "model";
    private static final String USER = "user";

    @TempDir
    Path tempDir;

    private Model<MultiClassNLRData, NodeClassificationTrainConfig> model;

    @BeforeEach
    void storeModel() throws IOException {

        var trainConfig = NodeClassificationTrainConfig.builder()
            .modelName(MODEL)
            .metrics(List.of(Metric.ACCURACY))
            .featureProperties(List.of("a", "b"))
            .targetProperty("t")
            .validationFolds(2)
            .holdoutFraction(0.19)
            .build();

        var modelData = MultiClassNLRData.builder()
            .weights(new Weights<>(Matrix.fill(0.19, 3, 4)))
            .classIdMap(new LocalIdMap())
            .build();

        model = Model.of(
            USER,
            MODEL,
            NodeClassificationTrain.MODEL_TYPE,
            GraphSchema.empty(),
            modelData,
            trainConfig
        );

        ModelToFileExporter.toFile(tempDir, model);
    }

    @Test
    void testLoadingMetaData() throws IOException {
        var storedModel = new StoredModel(tempDir);

        assertThat(storedModel.creator()).isEqualTo(model.creator());
        assertThat(storedModel.name()).isEqualTo(model.name());
        assertThat(storedModel.trainConfig()).isEqualTo(model.trainConfig());
        assertThat(storedModel.creationTime()).isEqualTo(model.creationTime());
        assertThat(storedModel.graphSchema()).isEqualTo(model.graphSchema());
        assertThat(storedModel.algoType()).isEqualTo(model.algoType());
        assertThat(storedModel.stored()).isTrue();
        assertThat(storedModel.loaded()).isFalse();
    }

    @Test
    void testLoadingData() throws IOException {
        var storedModel = new StoredModel(tempDir);

        storedModel.load();

        assertThat(storedModel.loaded()).isTrue();
        assertThat(storedModel.data()).isInstanceOf(MultiClassNLRData.class);
    }

    @Test
    void testUnLoadingData() throws IOException {
        var storedModel = new StoredModel(tempDir);

        storedModel.load();
        storedModel.unload();

        assertThat(storedModel.loaded()).isFalse();
        assertThatThrownBy(storedModel::data).hasMessage("The model 'model' is currently not loaded.");
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void testPublishStoredModel(boolean loadData) throws IOException {
        var persistedModel = new StoredModel(tempDir);
        if (loadData) {
            persistedModel.load();
        }
        persistedModel.publish();
        assertTrue(Files.exists(tempDir.resolve(META_DATA_FILE)));

        StoredModel publishedModel = new StoredModel(tempDir);
        assertEquals(model.name() + "_public", publishedModel.name());
        assertThat(publishedModel.sharedWith()).containsExactlyInAnyOrder(Model.ALL_USERS);

        if (loadData) {
            publishedModel.load();
        }
        assertThat(publishedModel)
            .usingRecursiveComparison()
            .ignoringFields("sharedWith", "name", "metaData")
            .isEqualTo(persistedModel);
    }
}
