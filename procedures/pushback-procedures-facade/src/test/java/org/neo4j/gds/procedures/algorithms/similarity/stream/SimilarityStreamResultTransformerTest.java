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
package org.neo4j.gds.procedures.algorithms.similarity.stream;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.procedures.algorithms.similarity.SimilarityStreamResult;
import org.neo4j.gds.similarity.SimilarityResult;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SimilarityStreamResultTransformerTest{

    @Test
    void shouldTransform(){
        var result = Stream.of(new SimilarityResult(1,2,1), new SimilarityResult(3,4,2));
        var graph = mock(Graph.class);
        when(graph.toOriginalNodeId(anyLong())).thenAnswer(invocation -> 2*(long)invocation.getArgument(0));

        var transformer = new SimilarityStreamResultTransformer(graph);

        assertThat(transformer.apply(result)).containsExactly(
            new SimilarityStreamResult(2,4,1),
            new SimilarityStreamResult(6,8,2)
        );
    }

}
