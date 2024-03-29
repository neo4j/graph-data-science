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
package org.neo4j.gds.procedures.community;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.algorithms.StreamComputationResult;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.wcc.WccStreamConfig;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class WccComputationResultTransformerTest {

    @Test
    void shouldReturnEmptyStreamForEmptyGraph() {
        // given
        StreamComputationResult<DisjointSetStruct> computationResult = StreamComputationResult.of(
            Optional.empty(),
            mock(Graph.class)
        );

        // when
        var wccStreamResult = WccComputationResultTransformer.toStreamResult(
            computationResult,
            mock(WccStreamConfig.class)
        );

        // then
        assertThat(wccStreamResult).isEmpty();
    }

}
