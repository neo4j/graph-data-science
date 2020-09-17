/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.model.catalog;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.core.GdsEdition;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.model.ModelCatalog;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.isA;
import static org.neo4j.graphalgo.compat.MapUtil.map;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

class ModelListProcTest extends BaseProcTest {

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(ModelListProc.class);
        GdsEdition.instance().setToEnterpriseEdition();
    }

    @AfterEach
    void tearDown() {
        ModelCatalog.removeAllLoadedModels();
    }

    @ParameterizedTest
    @ValueSource(strings = {"gds.beta.model.list()", "gds.beta.model.list(null)"})
    void listsModel(String query) {
        Model<String, TestTrainConfig> model1 = Model.of(getUsername(),"testModel1", "testAlgo1", "testData", TestTrainConfig.of());
        Model<Long, TestTrainConfig> model2 = Model.of(getUsername(),"testModel2", "testAlgo2", 1337L, TestTrainConfig.of());
        Model<Long, TestTrainConfig> otherUserModel = Model.of("anotherUser","testModel1337", "testAlgo1337", 3435L, TestTrainConfig.of());

        ModelCatalog.set(model1);
        ModelCatalog.set(model2);
        ModelCatalog.set(otherUserModel);

        assertCypherResult(
            formatWithLocale("CALL %s YIELD modelInfo, creationTime RETURN modelInfo, creationTime ORDER BY modelInfo.modelName", query),
            List.of(
                map(
                    "modelInfo", map("modelName", "testModel1", "modelType", "testAlgo1"),
                    "creationTime", isA(ZonedDateTime.class)
                ),
                map(
                    "modelInfo", map("modelName", "testModel2", "modelType", "testAlgo2"),
                    "creationTime", isA(ZonedDateTime.class)
                )
            )
        );
    }

    @Test
    void emptyListOnEmptyCatalog() {
        assertCypherResult(
            "CALL gds.beta.model.list()",
            List.of()
        );
    }

    @Test
    void returnSpecificModel() {
        Model<String, TestTrainConfig> model1 = Model.of(getUsername(),"testModel1", "testAlgo1", "testData", TestTrainConfig.of());
        Model<Long, TestTrainConfig> model2 = Model.of(getUsername(),"testModel2", "testAlgo2", 1337L, TestTrainConfig.of());
        ModelCatalog.set(model1);
        ModelCatalog.set(model2);

        assertCypherResult(
            "CALL gds.beta.model.list('testModel2')",
            singletonList(
                map(
                    "modelInfo", map("modelName", "testModel2", "modelType", "testAlgo2"),
                    "trainConfig", TestTrainConfig.of().toMap(),
                    "creationTime", isA(ZonedDateTime.class)
                )
            )
        );
    }

    @ParameterizedTest(name = "`{0}`")
    @MethodSource("invalidModelNames")
    void failOnEmptyModelName(String modelName) {
        assertError(
            "CALL gds.beta.model.list($modelName)",
            map("modelName", modelName),
            "can not be null or blank"
        );
    }

    static Stream<String> invalidModelNames() {
        return Stream.of("", "   ", "           ", "\r\n\t");
    }
}
