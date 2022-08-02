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
package org.neo4j.gds.config;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@GdlExtension
class AlgoBaseConfigTest {

    @GdlGraph
    public static final String GDL_GRAPH = "(:A)-[:REL]->(:B)-[:REL2]->(:C)";

    @Inject
    GraphStore graphStore;

    @Test
    void validateNodeLabels() {
        TestConfig config = TestConfigImpl.builder()
            .nodeLabels(List.of("A", "B", "X", "Y"))
            .build();

        assertThatThrownBy(() -> config.graphStoreValidation(
            graphStore,
            config.nodeLabelIdentifiers(graphStore),
            config.internalRelationshipTypes(graphStore)
        ))
            .hasMessageContaining("Could not find the specified `nodeLabels` of ['X', 'Y']. Available labels are ['A', 'B', 'C'].");
    }

    @Test
    void validateRelationshipTypes() {
        TestConfig config = TestConfigImpl.builder()
            .relationshipTypes(List.of("REL", "I_REL", "I_REL_2"))
            .build();

        assertThatThrownBy(() -> config.graphStoreValidation(
            graphStore,
            config.nodeLabelIdentifiers(graphStore),
            config.internalRelationshipTypes(graphStore)
        ))
            .hasMessageContaining(
                "Could not find the specified `relationshipTypes` of ['I_REL', 'I_REL_2']. Available relationship types are ['REL', 'REL2']."
            );
    }

    @Test
    void validateGraphFilter() {
        TestConfig config = TestConfigImpl.builder()
            .nodeLabels(List.of("A", "B", "X", "Y"))
            .relationshipTypes(List.of("REL", "I_REL", "I_REL_2"))
            .build();

        assertThatThrownBy(() -> config.graphStoreValidation(
            graphStore,
            config.nodeLabelIdentifiers(graphStore),
            config.internalRelationshipTypes(graphStore)
        ))
            .hasMessageContaining("Could not find the specified `nodeLabels` of ['X', 'Y']")
            .hasMessageContaining("Could not find the specified `relationshipTypes` of ['I_REL', 'I_REL_2']");
    }


    @Configuration
    interface TestConfig extends AlgoBaseConfig {
    }

}
