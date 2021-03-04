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
import org.junit.jupiter.api.io.TempDir;
import org.neo4j.configuration.Config;
import org.neo4j.gds.embeddings.graphsage.Layer;
import org.neo4j.gds.embeddings.graphsage.ModelData;
import org.neo4j.gds.embeddings.graphsage.SingleLabelFeatureFunction;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSage;
import org.neo4j.gds.embeddings.graphsage.algo.ImmutableGraphSageTrainConfig;
import org.neo4j.graphalgo.api.schema.GraphSchema;
import org.neo4j.graphalgo.compat.GraphDatabaseApiProxy;
import org.neo4j.graphalgo.core.ModelStoreSettings;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.graphalgo.junit.annotation.Edition;
import org.neo4j.graphalgo.junit.annotation.GdsEditionBeforeEachTestCase;
import org.neo4j.graphalgo.model.catalog.ModelDeleteProc;
import org.neo4j.graphalgo.model.catalog.ModelLoadProc;
import org.neo4j.graphalgo.model.catalog.ModelStoreProc;

import java.nio.file.Path;
import java.util.List;

@GdsEditionBeforeEachTestCase(Edition.EE)
class ModelCatalogStoreDocTest extends DocTestBase {

    @TempDir
    Path modelStoreLocation;

    @Override
    @BeforeEach
    void setUp() throws Exception {
        super.setUp();
        GraphDatabaseApiProxy
            .resolveDependency(db, Config.class)
            .set(ModelStoreSettings.model_store_location, modelStoreLocation);
        ModelCatalog.set(Model.of(
            getUsername(),
            "my-model",
            GraphSage.MODEL_TYPE,
            GraphSchema.empty(),
            ModelData.of(new Layer[0], new SingleLabelFeatureFunction()),
            ImmutableGraphSageTrainConfig.builder().modelName("my-model").degreeAsProperty(true).build(),
            Model.Mappable.EMPTY
        ));
    }

    @AfterEach
    void tearDown() {
        ModelCatalog.drop(getUsername(), "my-model");
    }

    @Override
    List<Class<?>> procedures() {
        return List.of(
            ModelStoreProc.class,
            ModelDeleteProc.class,
            ModelLoadProc.class
        );
    }

    @Override
    String adocFile() {
        return "model-catalog/model-catalog-store.adoc";
    }
}
