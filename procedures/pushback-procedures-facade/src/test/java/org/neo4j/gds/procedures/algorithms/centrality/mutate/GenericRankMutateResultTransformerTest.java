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
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.applications.algorithms.machinery.MutateNodePropertyService;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.pagerank.PageRankResult;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.scaling.Center;
import org.neo4j.gds.scaling.ScalerFactory;

import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class GenericRankMutateResultTransformerTest {

    @Test
    void shouldTransform() {
        var result = mock(PageRankResult.class);
        when(result.didConverge()).thenReturn(true);
        when(result.iterations()).thenReturn(30);
        when(result.centralityScoreProvider()).thenReturn(HugeDoubleArray.of(10,9,8)::get);
        when(result.nodePropertyValues()).thenReturn(mock(NodePropertyValues.class));
        var graphMock = mock(Graph.class);
        when(graphMock.nodeCount()).thenReturn(3L);

        var config = Map.of("a",(Object)("foo"));
        var  mutateService =  mock(MutateNodePropertyService.class);

        when(mutateService.mutateNodeProperties(
            any(Graph.class),
            any(GraphStore.class),
            anyString(),
            anyCollection(),
            any(NodePropertyValues.class))
        ).thenReturn(new NodePropertiesWritten(42));

        var transformer = new GenericRankMutateResultTransformer(
            mock(Graph.class),
            mock(GraphStore.class),
            config,
            ScalerFactory.parse(Center.TYPE),
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
                assertThat(mutate.didConverge()).isTrue();
                assertThat(mutate.ranIterations()).isEqualTo(30);
            });
    }

    @Test
    void shouldTransformButNotComputeHistogram() {
        var result = mock(PageRankResult.class);
        when(result.didConverge()).thenReturn(true);
        when(result.iterations()).thenReturn(30);
        when(result.centralityScoreProvider()).thenReturn(HugeDoubleArray.of(10,9,8)::get);
        when(result.nodePropertyValues()).thenReturn(mock(NodePropertyValues.class));
        var graphMock = mock(Graph.class);
        when(graphMock.nodeCount()).thenReturn(3L);

        var config = Map.of("a",(Object)("foo"));
        var  mutateService =  mock(MutateNodePropertyService.class);

        when(mutateService.mutateNodeProperties(
            any(Graph.class),
            any(GraphStore.class),
            anyString(),
            anyCollection(),
            any(NodePropertyValues.class))
        ).thenReturn(new NodePropertiesWritten(42));

        var transformer = new GenericRankMutateResultTransformer(
            mock(Graph.class),
            mock(GraphStore.class),
            config,
            ScalerFactory.parse(Center.TYPE),
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
                assertThat(mutate.postProcessingMillis()).isGreaterThanOrEqualTo(0);
                assertThat(mutate.configuration()).isEqualTo(config);
                assertThat(mutate.mutateMillis()).isGreaterThanOrEqualTo(0L);
                assertThat(mutate.centralityDistribution()).doesNotContainKey("p99");
                assertThat(mutate.nodePropertiesWritten()).isEqualTo(42L);
                assertThat(mutate.didConverge()).isTrue();
                assertThat(mutate.ranIterations()).isEqualTo(30);
            });
    }

}
