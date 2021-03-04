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
package org.neo4j.graphalgo.doc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.neo4j.graphalgo.api.schema.GraphSchema;
import org.neo4j.graphalgo.config.ModelConfig;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.graphalgo.junit.annotation.Edition;
import org.neo4j.graphalgo.junit.annotation.GdsEditionBeforeEachTestCase;
import org.neo4j.graphalgo.model.catalog.ModelDropProc;
import org.neo4j.graphalgo.model.catalog.ModelExistsProc;
import org.neo4j.graphalgo.model.catalog.ModelListProc;

import java.util.List;

@GdsEditionBeforeEachTestCase(Edition.EE)
class ModelCatalogDocTest extends DocTestBase {

    @Override
    @BeforeEach
    void setUp() throws Exception {
        super.setUp();
        ModelCatalog.set(Model.of(
            getUsername(),
            "my-model",
            "example-model-type",
            GraphSchema.empty(),
            new Object(),
            (ModelConfig) () -> "my-model",
            Model.Mappable.EMPTY
        ));
    }

    @AfterEach
    void tearDown() {
        ModelCatalog.removeAllLoadedModels();
    }

    @Override
    List<Class<?>> procedures() {
        return List.of(
            ModelListProc.class,
            ModelExistsProc.class,
            ModelDropProc.class
        );
    }

    @Override
    String adocFile() {
        return "model-catalog/model-catalog.adoc";
    }

}

