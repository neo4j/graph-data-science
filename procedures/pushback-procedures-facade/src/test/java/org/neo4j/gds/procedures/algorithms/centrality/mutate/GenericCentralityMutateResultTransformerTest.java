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
import org.neo4j.gds.algorithms.centrality.CentralityAlgorithmResult;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.applications.algorithms.machinery.MutateNodePropertyService;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.result.TimedAlgorithmResult;

import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class GenericCentralityMutateResultTransformerTest {

    @Test
    void shouldTransform() {
        var config = Map.of("a", (Object) ("foo"));
        var result = mock(CentralityAlgorithmResult.class);
        when(result.nodePropertyValues()).thenReturn(mock(NodePropertyValues.class));
        var  mutateService =  mock(MutateNodePropertyService.class);

        when(mutateService.mutateNodeProperties(
            any(Graph.class),
            any(GraphStore.class),
            anyString(),
            anyCollection(),
            any(NodePropertyValues.class))
        ).thenReturn(new NodePropertiesWritten(42));

        var transformer = new GenericCentralityMutateResultTransformer<>(
            mock(Graph.class),
            mock(GraphStore.class),
            config,
            true,
            new Concurrency(1),
            mutateService,
            Set.of("bar"),
            "foo"
        );

        var mutateResult = transformer.apply(new TimedAlgorithmResult<>(result, 10));

        assertThat(mutateResult.findFirst().orElseThrow())
            .satisfies(mutate -> {
                assertThat(mutate.preProcessingMillis()).isEqualTo(0);
                assertThat(mutate.computeMillis()).isEqualTo(10);
                assertThat(mutate.postProcessingMillis()).isGreaterThanOrEqualTo(0);
                assertThat(mutate.configuration()).isEqualTo(config);
                assertThat(mutate.mutateMillis()).isGreaterThanOrEqualTo(0L);
                assertThat(mutate.centralityDistribution()).containsKey("p99");
                assertThat(mutate.nodePropertiesWritten()).isEqualTo(42L);
            });
    }

    @Test
    void shouldTransformButNotComputeHistogram() {
        var config = Map.of("a", (Object) ("foo"));
        var result = mock(CentralityAlgorithmResult.class);
        when(result.nodePropertyValues()).thenReturn(mock(NodePropertyValues.class));

        var  mutateService =  mock(MutateNodePropertyService.class);

        when(mutateService.mutateNodeProperties(
            any(Graph.class),
            any(GraphStore.class),
            anyString(),
            anyCollection(),
            any(NodePropertyValues.class))
        ).thenReturn(new NodePropertiesWritten(42));

        var transformer = new GenericCentralityMutateResultTransformer<>(
            mock(Graph.class),
            mock(GraphStore.class),
            config,
            false,
            new Concurrency(1),
            mutateService,
            Set.of("bar"),
            "foo"
        );

        var mutateResult = transformer.apply(new TimedAlgorithmResult<>(result, 10));

        assertThat(mutateResult.findFirst().orElseThrow())
            .satisfies(mutate -> {
                assertThat(mutate.preProcessingMillis()).isEqualTo(0);
                assertThat(mutate.computeMillis()).isEqualTo(10);
                assertThat(mutate.postProcessingMillis()).isEqualTo(0);
                assertThat(mutate.configuration()).isEqualTo(config);
                assertThat(mutate.mutateMillis()).isGreaterThanOrEqualTo(0L);
                assertThat(mutate.centralityDistribution().containsKey("p99")).isFalse();
                assertThat(mutate.nodePropertiesWritten()).isEqualTo(42L);
            });
    }

}
