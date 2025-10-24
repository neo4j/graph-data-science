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
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.modularity.CommunityModularity;
import org.neo4j.gds.modularity.ModularityResult;
import org.neo4j.gds.procedures.algorithms.community.ModularityStreamResult;
import org.neo4j.gds.result.TimedAlgorithmResult;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ModularityStreamTransformerTest {

    @Test
    void shouldTransformResult(){

        var result = mock(ModularityResult.class);
        when(result.modularityScores()).thenReturn(HugeObjectArray.of(new CommunityModularity(0,1), new CommunityModularity(2,2)));
        when(result.communityCount()).thenReturn(2L);

        var transformer = new ModularityStreamTransformer();

        var stream = transformer.apply(new TimedAlgorithmResult<>(result, -1));
        assertThat(stream).containsExactlyInAnyOrder(
            new ModularityStreamResult(0,1d),
            new ModularityStreamResult(2,2d)
        );

    }

    @Test
    void shouldTransformEmptyResult(){

        var transformer = new ModularityStreamTransformer();
        var stream = transformer.apply(new TimedAlgorithmResult<>(ModularityResult.EMPTY,-1));
        assertThat(stream).isEmpty();
    }

}
