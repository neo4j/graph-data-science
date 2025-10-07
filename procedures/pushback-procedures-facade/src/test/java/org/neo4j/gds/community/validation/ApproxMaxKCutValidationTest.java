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
package org.neo4j.gds.community.validation;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.GraphStore;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ApproxMaxKCutValidationTest {

    @Test
    void shouldNotThrowIfMinCommunitySizesIsValid(){

        var graphStore = mock(GraphStore.class);
        when(graphStore.nodeCount()).thenReturn(202L);
        var validation = new ApproxMaxKCutValidation(List.of(100L));

        assertThatNoException().isThrownBy(() -> validation.validateMinCommunitySizesSum(graphStore));
    }

    @Test
    void shouldThrowIfMinCommunitySizesIsLarge(){

        var graphStore = mock(GraphStore.class);
        when(graphStore.nodeCount()).thenReturn(2L);
        var validation = new ApproxMaxKCutValidation(List.of(100L));

        assertThatThrownBy(()-> validation.validateMinCommunitySizesSum(graphStore))
            .hasMessageContaining("The sum of min community sizes is larger than half of the number of nodes in the graph: 100 > 1");
    }
}
