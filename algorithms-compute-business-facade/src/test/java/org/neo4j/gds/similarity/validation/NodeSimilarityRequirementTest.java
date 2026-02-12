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
package org.neo4j.gds.similarity.validation;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.GraphStore;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThatException;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class NodeSimilarityRequirementTest {

    @Test
    void shouldNotValidateIfNotWccSet(){
        var graphStore = mock(GraphStore.class);
        when(graphStore.hasNodeProperty(ArgumentMatchers.anyCollection(),anyString())).thenReturn(false);
        var  validation = new NodeSimilarityRequirement(
            false,
            "p"
        );

        assertThatNoException().isThrownBy(() -> validation.validate(
            graphStore,
            Set.of(NodeLabel.of("Node")),
            null
        ));
    }

    @Test
    void shouldValidateIfNotSet(){
        var graphStore = mock(GraphStore.class);
        when(graphStore.hasNodeProperty(ArgumentMatchers.anyCollection(),anyString())).thenReturn(false);
        var  validation = new NodeSimilarityRequirement(
            true,
            "p"
        );

        assertThatException().isThrownBy(() -> validation.validate(
            graphStore,
            Set.of(NodeLabel.of("Node")),
            null
        ));
    }


}
