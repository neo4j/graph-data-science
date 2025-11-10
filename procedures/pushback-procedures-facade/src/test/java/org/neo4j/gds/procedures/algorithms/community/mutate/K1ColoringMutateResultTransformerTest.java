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
package org.neo4j.gds.procedures.algorithms.community.mutate;

import com.carrotsearch.hppc.BitSet;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.algorithms.machinery.MutateNodePropertyService;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.k1coloring.K1ColoringResult;
import org.neo4j.gds.result.TimedAlgorithmResult;

import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class K1ColoringMutateResultTransformerTest {

    @Test
    void shouldTransformResultAndComputeUsedColors() {

        var config = Map.of("a", (Object) ("foo"));
        var result = mock(K1ColoringResult.class);
        when(result.didConverge()).thenReturn(true);
        when(result.ranIterations()).thenReturn(100L);

        var bitset = new BitSet(10);
        bitset.set(2);
        bitset.set(4);
        when(result.usedColors()).thenReturn(bitset);
        when(result.colors()).thenReturn(HugeLongArray.of(1, 2, 3));

        var  mutateService =  mock(MutateNodePropertyService.class);

        var transformer = new K1ColoringMutateResultTransformer(config,
            true,
            mutateService,
            Set.of("bar"),
            "foo",
            mock(Graph.class),
            mock(GraphStore.class));

        var mutateResult = transformer.apply(new TimedAlgorithmResult<>(result, 10));

        assertThat(mutateResult.findFirst().orElseThrow())
            .satisfies(mutate -> {
                assertThat(mutate.preProcessingMillis()).isEqualTo(0);
                assertThat(mutate.computeMillis()).isEqualTo(10);
                assertThat(mutate.mutateMillis()).isNotNegative();

                assertThat(mutate.nodeCount()).isEqualTo(3);
                assertThat(mutate.colorCount()).isEqualTo(2);
                assertThat(mutate.ranIterations()).isEqualTo(100);
                assertThat(mutate.didConverge()).isTrue();
                assertThat(mutate.configuration()).isEqualTo(config);
            });
    }

}
