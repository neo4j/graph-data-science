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

import org.assertj.core.data.TemporalUnitWithinOffset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.model.storage.ModelToFileExporter;
import org.neo4j.gds.config.ModelConfig;
import org.neo4j.gds.core.model.Model;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.time.temporal.ChronoUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.model.storage.ModelToFileExporter.META_DATA_FILE;

abstract class BaseStoreModelTest<DATA, CONFIG extends ModelConfig> {
    static final String MODEL = "model";
    static final String USER = "user";

    @TempDir
    Path tempDir;

    private Model<DATA, CONFIG> model;

    @BeforeEach
    void storeModel() throws IOException {
        model = model();
        ModelToFileExporter.toFile(tempDir, model());
    }

    @Test
    abstract void testLoadingData() throws IOException;

    abstract Model<DATA, CONFIG> model();

    @Test
    void testLoadingMetaData() throws IOException {
        var storedModel = new StoredModel(tempDir);

        assertThat(storedModel.creator()).isEqualTo(model.creator());
        assertThat(storedModel.name()).isEqualTo(model.name());
        assertThat(storedModel.trainConfig())
            .usingRecursiveComparison()
            .ignoringFields("params")
            .isEqualTo(model.trainConfig());
        assertThat(storedModel.creationTime())
            .isCloseTo(model.creationTime(), new TemporalUnitWithinOffset(1, SECONDS));
        assertThat(storedModel.graphSchema()).isEqualTo(model.graphSchema());
        assertThat(storedModel.algoType()).isEqualTo(model.algoType());
        assertThat(storedModel.stored()).isTrue();
        assertThat(storedModel.loaded()).isFalse();
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
    @ValueSource(booleans = {true, false})
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
