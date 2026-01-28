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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.applications.algorithms.machinery.MutateNodePropertyService;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.cliqueCounting.CliqueCountingResult;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.result.TimedAlgorithmResult;

import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CliqueCountingMutateResultTransformerTest {

    @Test
    void shouldTransformResult(){

        var config = Map.of("a",(Object)("foo"));
        var result = mock(CliqueCountingResult.class);
        when(result.globalCount()).thenReturn(new long[]{1,2,3});
        when(result.perNodeCount()).thenReturn(HugeObjectArray.of(new long[]{1},new long[]{2}));

        var  mutateService =  mock(MutateNodePropertyService.class);

        when(mutateService.mutateNodeProperties(
            any(Graph.class),
            any(GraphStore.class),
            anyString(),
            anyCollection(),
            any(NodePropertyValues.class))
        ).thenReturn(new NodePropertiesWritten(42));

        var transformer = new CliqueCountingMutateResultTransformer(
            config,
            mutateService,
            Set.of("bar"),
            "foo",
        mock(Graph.class),
        mock(GraphStore.class)
        );

        var mutateResult = transformer.apply(new TimedAlgorithmResult<>(result, 10));

        assertThat(mutateResult.findFirst().orElseThrow())
            .satisfies(mutate -> {
                assertThat(mutate.preProcessingMillis()).isEqualTo(0);
                assertThat(mutate.computeMillis()).isEqualTo(10);
                assertThat(mutate.globalCount()).containsExactly(1L, 2L, 3L);
                assertThat(mutate.mutateMillis()).isNotNegative();
                assertThat(mutate.nodePropertiesWritten()).isEqualTo(42L);
                assertThat(mutate.configuration()).isEqualTo(config);
            });

    }

}
