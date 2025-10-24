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
package org.neo4j.gds.procedures.algorithms.community.stream;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.collections.hsa.HugeSparseDoubleArray;
import org.neo4j.gds.conductance.ConductanceResult;
import org.neo4j.gds.procedures.algorithms.community.ConductanceStreamResult;
import org.neo4j.gds.result.TimedAlgorithmResult;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ConductanceStreamTransformerTest {

    @Test
    void shouldTransformResult(){
        var builder = HugeSparseDoubleArray.builder(Double.NaN);
        builder.setIfAbsent(0,1);
        builder.setIfAbsent(2,2);
        var array= builder.build();
        var result = mock(ConductanceResult.class);
        when(result.communityConductances()).thenReturn(array);

        var transformer = new ConductanceStreamTransformer();

        var stream = transformer.apply(new TimedAlgorithmResult<>(result, -1));
        assertThat(stream).containsExactlyInAnyOrder(
            new ConductanceStreamResult(0,1d),
            new ConductanceStreamResult(2,2d)
        );

    }

    @Test
    void shouldTransformEmptyResult(){

        var transformer = new ConductanceStreamTransformer();
        var stream = transformer.apply(new TimedAlgorithmResult<>(ConductanceResult.EMPTY, -1));
        assertThat(stream).isEmpty();
    }

}
