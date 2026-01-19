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
import org.neo4j.gds.hdbscan.Labels;
import org.neo4j.gds.result.TimedAlgorithmResult;

import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class HDBScanMutateResultTransformerTest {

    @Test
    void shouldTransform() {
        var config = Map.of("a", (Object) ("foo"));
        var result = mock(Labels.class);
        when(result.numberOfClusters()).thenReturn(20L);
        when(result.numberOfNoisePoints()).thenReturn(100L);

        var  mutateService =  mock(MutateNodePropertyService.class);

        when(mutateService.mutateNodeProperties(
            any(Graph.class),
            any(GraphStore.class),
            any(MutateNodePropertyService.MutateNodePropertySpec.class),
            any(NodePropertyValues.class))
        ).thenReturn(new NodePropertiesWritten(42));

        var transformer = new HDBScanMutateResultTransformer(
            config,
            3L,
            mutateService,
            Set.of("bar"),
            "foo",
            mock(Graph.class),
            mock(GraphStore.class)
        );

        var mutateResult = transformer.apply(new TimedAlgorithmResult<>(result, 10));

        assertThat(mutateResult.findFirst().orElseThrow())
            .satisfies(mutate -> {
                assertThat(mutate.nodeCount()).isEqualTo(3L);
                assertThat(mutate.numberOfClusters()).isEqualTo(20L);
                assertThat(mutate.numberOfNoisePoints()).isEqualTo(100L);
                assertThat(mutate.preProcessingMillis()).isEqualTo(0);
                assertThat(mutate.computeMillis()).isEqualTo(10);
                assertThat(mutate.postProcessingMillis()).isEqualTo(0);
                assertThat(mutate.configuration()).isEqualTo(config);
                assertThat(mutate.mutateMillis()).isNotNegative();
                assertThat(mutate.nodePropertiesWritten()).isEqualTo(42L);
            });
    }

}
