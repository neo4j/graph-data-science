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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.neo4j.configuration.Config;
import org.neo4j.gds.model.catalog.ModelPublishProc;
import org.neo4j.graphalgo.compat.GraphDatabaseApiProxy;
import org.neo4j.graphalgo.core.ModelStoreSettings;
import org.neo4j.graphalgo.junit.annotation.Edition;
import org.neo4j.graphalgo.junit.annotation.GdsEditionTest;

import java.nio.file.Path;
import java.util.List;

@GdsEditionTest(Edition.EE)
class ModelCatalogPublishDocTest extends ModelCatalogDocTest {

    @TempDir
    Path modelStoreLocation;

    @BeforeEach
    void setModelStoreLocation() {
        GraphDatabaseApiProxy
            .resolveDependency(db, Config.class)
            .set(ModelStoreSettings.model_store_location, modelStoreLocation);
    }

    @Override
    List<Class<?>> procedures() {
        return List.of(ModelPublishProc.class);
    }

    @Override
    String adocFile() {
        return "model-catalog/model-catalog-publish.adoc";
    }
}
