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
package org.neo4j.gds.model.catalog;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.model.TestCustomInfo;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jModelCatalogExtension;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.isA;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@Neo4jModelCatalogExtension
class ModelListProcTest extends ModelProcBaseTest {

    @Inject
    private ModelCatalog modelCatalog;

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(ModelListProc.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"gds.model.list()", "gds.model.list(null)"})
    void listsModel(String query) {
        var model1 = Model.of(
            "testAlgo1",
            GRAPH_SCHEMA,
            "testData",
            TestTrainConfig.of(getUsername(), "testModel1"),
            new TestCustomInfo()
        );
        var model2 = Model.of(
            "testAlgo2",
            GRAPH_SCHEMA,
            1337L,
            TestTrainConfig.of(getUsername(), "testModel2"),
            new TestCustomInfo()
        );

        var otherUserModel = Model.of(
            "testAlgo1337",
            GRAPH_SCHEMA,
            3435L,
            TestTrainConfig.of("anotheruser", "testModel"),
            new TestCustomInfo()
        );

        modelCatalog.set(model1);
        modelCatalog.set(model2);
        modelCatalog.set(otherUserModel);

        assertCypherResult(
            formatWithLocale(
                "CALL %s YIELD modelInfo, graphSchema, trainConfig, loaded, stored, creationTime, published " +
                "RETURN modelInfo, graphSchema, trainConfig, loaded, stored, creationTime, published " +
                "ORDER BY modelInfo.modelName",
                query
            ),
            List.of(
                Map.of(
                    "modelInfo", Map.of("modelName", "testModel1", "modelType", "testAlgo1"),
                    "graphSchema", EXPECTED_SCHEMA,
                    "trainConfig", TestTrainConfig.of(getUsername(), "testModel1").toMap(),
                    "loaded", true,
                    "stored", false,
                    "creationTime", isA(ZonedDateTime.class),
                    "published", false
                ),
                Map.of(
                    "modelInfo", Map.of("modelName", "testModel2", "modelType", "testAlgo2"),
                    "graphSchema", EXPECTED_SCHEMA,
                    "trainConfig", TestTrainConfig.of(getUsername(), "testModel2").toMap(),
                    "loaded", true,
                    "stored", false,
                    "creationTime", isA(ZonedDateTime.class),
                    "published", false
                )
            )
        );
    }

    @Test
    void betaProcWithOldColumns() {
        var model = Model.of(
            "testAlgo1",
            GRAPH_SCHEMA,
            "testData",
            TestTrainConfig.of(getUsername(), "testModel1"),
            new TestCustomInfo()
        );

        modelCatalog.set(model);

        assertCypherResult(
            "CALL gds.beta.model.list()",
            List.of(
                Map.of(
                    "modelInfo", Map.of("modelName", "testModel1", "modelType", "testAlgo1"),
                    "graphSchema", EXPECTED_SCHEMA,
                    "trainConfig", TestTrainConfig.of(getUsername(), "testModel1").toMap(),
                    "loaded", true,
                    "stored", false,
                    "creationTime", isA(ZonedDateTime.class),
                    "shared", false
                )
            )
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "CALL gds.model.list()",
        "CALL gds.model.list('someModel')"
    })
    void emptyResultOnListQueries(String query) {
        assertCypherResult(
            query,
            List.of()
        );
    }

    @Test
    void returnSpecificModel() {
        var model1 = Model.of(
            "testAlgo1",
            GRAPH_SCHEMA,
            "testData",
            TestTrainConfig.of(getUsername(), "testModel1"),
            new TestCustomInfo()
        );

        var model2 = Model.of(
            "testAlgo2",
            GRAPH_SCHEMA,
            1337L,
            TestTrainConfig.of(getUsername(), "testModel2"),
            new TestCustomInfo()
        );
        modelCatalog.set(model1);
        modelCatalog.set(model2);

        assertCypherResult(
            "CALL gds.model.list('testModel2')",
            singletonList(
                Map.of(
                    "modelInfo", Map.of("modelName", "testModel2", "modelType", "testAlgo2"),
                    "trainConfig", TestTrainConfig.of(getUsername(), "testModel2").toMap(),
                    "graphSchema", EXPECTED_SCHEMA,
                    "loaded", true,
                    "stored", false,
                    "creationTime", isA(ZonedDateTime.class),
                    "published", false
                )
            )
        );
    }

    @ParameterizedTest(name = "`{0}`")
    @MethodSource("invalidModelNames")
    void failOnEmptyModelName(String modelName) {
        assertError(
            "CALL gds.model.list($modelName)",
            Map.of("modelName", modelName),
            "can not be null or blank"
        );
    }

    static Stream<String> invalidModelNames() {
        return Stream.of("", "   ", "           ", "\r\n\t");
    }
}
