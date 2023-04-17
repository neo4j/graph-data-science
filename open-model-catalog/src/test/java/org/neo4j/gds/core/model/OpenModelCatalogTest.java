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
package org.neo4j.gds.core.model;

import org.eclipse.collections.impl.Counter;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.config.BaseConfig;
import org.neo4j.gds.core.model.Model.CustomInfo;
import org.neo4j.gds.gdl.GdlFactory;
import org.neo4j.gds.model.ModelConfig;
import org.neo4j.gds.model.catalog.TestTrainConfig;

import java.util.Locale;
import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ModelCatalogExtension
class OpenModelCatalogTest {

    private static final String USERNAME = "testUser";
    private static final GraphSchema GRAPH_SCHEMA = GdlFactory.of("(:Node1)").build().schema();

    private static final Model<String, TestTrainConfig, CustomInfo> TEST_MODEL = Model.of(
        "testAlgo",
        GRAPH_SCHEMA,
        "modelData",
        TestTrainConfig.of(USERNAME, "testModel"),
        new TestCustomInfo()
    );

    @InjectModelCatalog
    private ModelCatalog modelCatalog;

    @Test
    void shouldNotStoreMoreThanAllowedModels() {
        int allowedModelsCount = 3;

        for (int i = 0; i < allowedModelsCount; i++) {
            int modelIndex = i;
            assertDoesNotThrow(() -> {
                modelCatalog.set(Model.of(
                    "testAlgo",
                    GRAPH_SCHEMA,
                    1337L,
                    TestTrainConfig.of(USERNAME, "testModel_" + modelIndex),
                    new TestCustomInfo()
                ));
            });
        }

        var tippingModel = Model.of(
            "testAlgo",
            GRAPH_SCHEMA,
            1337L,
            TestTrainConfig.of(USERNAME, "testModel_" + (allowedModelsCount + 1)),
            new TestCustomInfo()
        );

        assertThatThrownBy(() -> modelCatalog.set(tippingModel))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining(String.format(
                    Locale.ENGLISH,
                    "Storing more than `%d` models in the catalog is not available in openGDS.",
                    allowedModelsCount
                )
            );
    }

    @Test
    void shouldStoreModelsPerType() {
        var model = Model.of(
            "testAlgo",
            GRAPH_SCHEMA,
            "testTrainData",
            TestTrainConfig.of(USERNAME, "testModel"),
            new TestCustomInfo()
        );
        var model2 = Model.of("testAlgo2", GRAPH_SCHEMA, 1337L, TestTrainConfig.of(USERNAME, "testModel2"), new TestCustomInfo());

        modelCatalog.set(model);
        modelCatalog.set(model2);

        assertEquals(
            model,
            modelCatalog.get(USERNAME, "testModel", String.class, TestTrainConfig.class, CustomInfo.class)
        );
        assertEquals(
            model2,
            modelCatalog.get(USERNAME, "testModel2", Long.class, TestTrainConfig.class, CustomInfo.class)
        );
    }

    @Test
    void shouldNotifyListeners() {
        var counter = new Counter();
        modelCatalog.registerListener(model -> counter.increment());

        var model = Model.of(
            "testAlgo",
            GRAPH_SCHEMA,
            "testTrainData",
            TestTrainConfig.of(USERNAME, "testModel"),
            new TestCustomInfo()
        );

        assertThat(counter.getCount()).isEqualTo(0);

        modelCatalog.set(model);

        assertThat(counter.getCount()).isEqualTo(1);

        assertThatThrownBy(() -> modelCatalog.set(model));
        // not be called if the set is not successful
        assertThat(counter.getCount()).isEqualTo(1);
    }

    @Test
    void shouldThrowWhenPublishing() {
        modelCatalog.set(TEST_MODEL);

        assertThatThrownBy(() -> modelCatalog.publish(USERNAME, TEST_MODEL.name()))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Publishing models is not available in openGDS.");
    }

    @Test
    void shouldStoreModels() {
        var model = Model.of(
            "testAlgo",
            GRAPH_SCHEMA,
            "testTrainData",
            TestTrainConfig.of("user1", "testModel"),
            new TestCustomInfo()
        );
        var model2 = Model.of(
            "testAlgo",
            GRAPH_SCHEMA,
            "testTrainData",
            TestTrainConfig.of("user2", "testModel2"),
            new TestCustomInfo()
        );

        modelCatalog.set(model);
        modelCatalog.set(model2);

        assertEquals(
            model,
            modelCatalog.get("user1", "testModel", String.class, TestTrainConfig.class, CustomInfo.class)
        );
        assertEquals(
            model2,
            modelCatalog.get("user2", "testModel2", String.class, TestTrainConfig.class, CustomInfo.class)
        );
    }

    @Test
    void shouldCountModels() {
        var model1 = Model.of(
            "testAlgo1",
            GRAPH_SCHEMA,
            "modelData1",
            TestTrainConfig.of(USERNAME, "testModel1"),
            new TestCustomInfo()
        );

        var model2 = Model.of(
            "testAlgo2",
            GRAPH_SCHEMA,
            1337L,
            TestTrainConfig.of(USERNAME, "testModel2"),
            new TestCustomInfo()
        );

        var publicModel = Model.of(
            "testAlgo2",
            GRAPH_SCHEMA,
            1337L,
            TestTrainConfig.of("anotherUser", "testModel2"),
            new TestCustomInfo()
        );

        assertThat(modelCatalog.modelCount()).isEqualTo(0);

        modelCatalog.set(model1);

        assertThat(modelCatalog.modelCount()).isEqualTo(1);

        modelCatalog.set(model2);

        assertThat(modelCatalog.modelCount()).isEqualTo(2);

        modelCatalog.set(publicModel);

        assertThat(modelCatalog.modelCount()).isEqualTo(3);
    }

    @Test
    void shouldThrowWhenTryingToGetOtherUsersModel() {
        modelCatalog.set(TEST_MODEL);

        var ex = assertThrows(
            NoSuchElementException.class,
            () -> modelCatalog.get("fakeUser", "testModel", String.class, TestTrainConfig.class, CustomInfo.class)
        );

        assertEquals("Model with name `testModel` does not exist.", ex.getMessage());
        assertNotNull(TEST_MODEL.creationTime());
    }

    @Test
    void shouldThrowOnModelDataTypeMismatch() {
        modelCatalog.set(TEST_MODEL);

        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> modelCatalog.get(USERNAME, "testModel", Double.class, TestTrainConfig.class, CustomInfo.class)
        );

        assertEquals(
            "The model `testModel` has data with different types than expected. " +
            "Expected data type: `java.lang.String`, invoked with model data type: `java.lang.Double`.",
            ex.getMessage()
        );
    }

    @Test
    void shouldThrowOnModelConfigTypeMismatch() {
        modelCatalog.set(TEST_MODEL);

        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> modelCatalog.get(
                USERNAME,
                "testModel",
                String.class,
                ModelCatalogTestTrainConfig.class,
                CustomInfo.class
            )
        );

        assertEquals(
            "The model `testModel` has a training config with different types than expected. " +
            "Expected train config type: `org.neo4j.gds.model.catalog.TestTrainConfigImpl`, " +
            "invoked with model config type: `org.neo4j.gds.core.model.OpenModelCatalogTest$ModelCatalogTestTrainConfig`.",
            ex.getMessage()
        );
    }

    @Test
    void shouldThrowIfModelNameAlreadyExists() {
        modelCatalog.set(TEST_MODEL);

        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> modelCatalog.verifyModelCanBeStored(TEST_MODEL.creator(), TEST_MODEL.name(), TEST_MODEL.algoType())
        );

        assertEquals(
            "Model with name `testModel` already exists.",
            ex.getMessage()
        );
    }

    @Test
    void shouldThrowIfModelNameAlreadyExistsOnSet() {
        modelCatalog.set(TEST_MODEL);

        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> modelCatalog.set(TEST_MODEL)
        );

        assertEquals(
            "Model with name `testModel` already exists.",
            ex.getMessage()
        );
    }

    @Test
    void checksIfModelExists() {
        modelCatalog.set(TEST_MODEL);

        assertTrue(modelCatalog.exists(USERNAME, "testModel"));
        assertFalse(modelCatalog.exists(USERNAME, "bogusModel"));
        assertFalse(modelCatalog.exists("fakeUser", "testModel"));
    }

    @Test
    void shouldDropModel() {
        modelCatalog.set(TEST_MODEL);

        assertTrue(modelCatalog.exists(USERNAME, "testModel"));
        modelCatalog.dropOrThrow(USERNAME, "testModel");
        assertFalse(modelCatalog.exists(USERNAME, "testModel"));
    }

    @Test
    void shouldNotThrowWhenListingNonExistentModel() {
        assertDoesNotThrow(() -> modelCatalog.getUntyped(USERNAME, "nonExistentModel"));
    }

    @Test
    void shouldReturnEmptyList() {
        assertEquals(0, modelCatalog.list(USERNAME).size());
    }

    @ValueClass
    @Configuration("ModelCatalogTestTrainConfigImpl")
    @SuppressWarnings("immutables:subtype")
    interface ModelCatalogTestTrainConfig extends BaseConfig, ModelConfig {

        long serialVersionUID = 0x42L;

        static ModelCatalogTestTrainConfig of() {
            return ImmutableModelCatalogTestTrainConfig.of("username", "modelName");
        }
    }

}
