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
package org.neo4j.gds.procedures.algorithms.centrality.mutate;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.algorithms.machinery.MutateNodePropertyService;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.beta.pregel.NodeValue;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.hits.HitsResultWithGraph;
import org.neo4j.gds.result.TimedAlgorithmResult;

import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class HitsMutateResultTransformerTest {

    @Test
    void shouldTransform() {
        var config = Map.of("a", (Object) ("foo"));
        var auth = HugeDoubleArray.of(10, 9, 8);
        var hub = HugeDoubleArray.of(7, 6, 5);
        var nodeValues = mock(NodeValue.class);
        when(nodeValues.doubleProperties(eq("auth"))).thenReturn(auth);
        when(nodeValues.doubleProperties(eq("hub"))).thenReturn(hub);
        var pregelResult = mock(PregelResult.class);
        when(pregelResult.ranIterations()).thenReturn(1234);
        when(pregelResult.didConverge()).thenReturn(true);
        when(pregelResult.nodeValues()).thenReturn(nodeValues);
        var result = mock(HitsResultWithGraph.class);
        when(result.pregelResult()).thenReturn(pregelResult);
        when(result.graph()).thenReturn(mock(Graph.class));

        var  mutateService =  mock(MutateNodePropertyService.class);

        when(mutateService.mutateNodeProperties(
            any(Graph.class),
            any(GraphStore.class),
            anyList(),
            anySet()
        )).thenReturn(new NodePropertiesWritten(42));

        var transformer = new HitsMutateResultTransformer(
            mock(GraphStore.class),
            config,
            mutateService,
            Set.of("bar"),
            "auth",
            "hub",
            "foo"
        );

        var mutateResult = transformer.apply(new TimedAlgorithmResult<>(result, 10));

        assertThat(mutateResult.findFirst().orElseThrow())
            .satisfies(mutate -> {
                assertThat(mutate.computeMillis()).isEqualTo(10);
                assertThat(mutate.configuration()).isEqualTo(config);
                assertThat(mutate.mutateMillis()).isGreaterThanOrEqualTo(0L);
                assertThat(mutate.didConverge()).isTrue();
                assertThat(mutate.ranIterations()).isEqualTo(1234);
                assertThat(mutate.nodePropertiesWritten()).isEqualTo(42L);
            });
    }

}
