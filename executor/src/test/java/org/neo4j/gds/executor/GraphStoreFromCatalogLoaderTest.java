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
package org.neo4j.gds.executor;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.extension.BaseGdlSupportExtension.DATABASE_ID;

@GdlExtension
class GraphStoreFromCatalogLoaderTest {

    @GdlGraph(addToCatalog = true)
    static String GDL =
        "(a:A {p: 1, q: [2.1, 3.2, 10.0]})";

    @Test
    void testGraphDimensions() {
        var loader = new GraphStoreFromCatalogLoader(
            "graph",
            new TestAlgoBaseConfig(),
            "",
            DATABASE_ID,
            false
        );

        assertThat(loader.graphDimensions().nodePropertyDimensions().get("p")).contains(1);
        assertThat(loader.graphDimensions().nodePropertyDimensions().get("q")).contains(3);
        assertThat(loader.graphDimensions().nodePropertyDimensions().get("NOTHERE")).isEmpty();
    }


    static class TestAlgoBaseConfig implements AlgoBaseConfig {

        @Override
        public Optional<String> usernameOverride() {
            return Optional.empty();
        }
    }
}
