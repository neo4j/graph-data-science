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
package org.neo4j.gds.core.loading.validation;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.loading.Capabilities;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PregelPropertiesRequirementTest {

    @Test
    void shouldNotThrowForBlankWriteProperty() {
        var requirement = new PregelPropertiesRequirement("", false);

        assertThatNoException().isThrownBy(() -> requirement.onGraphStoreFiltered(
            graphStoreWith(Capabilities.WriteMode.NONE),
            List.of(),
            List.of(),
            Optional.empty()
        ));
    }

    @Test
    void shouldThrowForUnwritableGraphWhenWritingToDatabase() {
        var requirement = new PregelPropertiesRequirement("community", false);

        assertThatThrownBy(() -> requirement.onGraphStoreFiltered(
            graphStoreWith(Capabilities.WriteMode.NONE),
            List.of(),
            List.of(),
            Optional.empty()
        )).hasMessageContaining("does not support `write` execution mode");
    }

    @Test
    void shouldNotThrowForUnwritableGraphWhenWritingToResultStore() {
        var requirement = new PregelPropertiesRequirement("community", true);

        assertThatNoException().isThrownBy(() -> requirement.onGraphStoreFiltered(
            graphStoreWith(Capabilities.WriteMode.NONE),
            List.of(),
            List.of(),
            Optional.empty()
        ));
    }

    @Test
    void shouldNotThrowForWritableGraph() {
        var requirement = new PregelPropertiesRequirement("community", false);

        assertThatNoException().isThrownBy(() -> requirement.onGraphStoreFiltered(
            graphStoreWith(Capabilities.WriteMode.REMOTE),
            List.of(),
            List.of(),
            Optional.empty()
        ));
    }

    private static GraphStore graphStoreWith(Capabilities.WriteMode writeMode) {
        var graphStore = mock(GraphStore.class);
        when(graphStore.capabilities()).thenReturn(new Capabilities(writeMode));
        return graphStore;
    }
}
