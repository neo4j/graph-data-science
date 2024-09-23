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
package org.neo4j.gds.applications.algorithms.machinery;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.core.loading.GraphResources;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.Dijkstra;

/**
 * Pinky promise: next time we add or change functionality here,
 * we break this class apart so that we can test it more sensibly. Smaller chunks, more readable class,
 * and tests that are not pages long.
 */
class DefaultAlgorithmProcessingTemplateTest {
    @Test
    void shouldDoFourStepProcess() {
        var graphStoreCatalogService = mock(GraphStoreCatalogService.class);
        var requestScopedDependencies = RequestScopedDependencies.builder().build();
        var algorithmComputer = mock(ComputationService.class);
        var template = new DefaultAlgorithmProcessingTemplate(
            graphStoreCatalogService,
            requestScopedDependencies,
            algorithmComputer
        );

        var configuration = new ExampleConfiguration();
        var graphResources = new GraphResources(null, mock(Graph.class), null);
        when(graphStoreCatalogService.getGraphResources(
            GraphName.parse("some graph"),
            configuration,
            Optional.empty(),
            Optional.empty(),
            requestScopedDependencies.getUser(),
            requestScopedDependencies.getDatabaseId()
        )).thenReturn(graphResources);
        when(algorithmComputer.computeAlgorithm(
            configuration,
            graphResources,
            Dijkstra,
            null,
            null
        )).thenReturn("some result");
        Object renderedResult = template.processAlgorithmAndAnySideEffects(
            Optional.empty(),
            GraphName.parse("some graph"),
            configuration,
            Optional.empty(),
            Dijkstra,
            null,
            null,
            Optional.of((__, ___) -> Optional.of("metadata from some side effect")),
            (__, result, timings, metadata) -> {
                assertThat(result).hasValue("some result");
                assertThat(timings.preProcessingMillis).isGreaterThan(-1);
                assertThat(timings.computeMillis).isGreaterThan(-1);
                assertThat(timings.mutateOrWriteMillis).isGreaterThan(-1);
                assertThat(metadata).hasValue("metadata from some side effect");

                return "some rendered result";
            }
        );

        assertThat(renderedResult).isEqualTo("some rendered result");
    }

    @Test
    void shouldSkipSideEffect() {
        var graphStoreCatalogService = mock(GraphStoreCatalogService.class);
        var requestScopedDependencies = RequestScopedDependencies.builder().build();
        var algorithmComputer = mock(ComputationService.class);
        var template = new DefaultAlgorithmProcessingTemplate(
            graphStoreCatalogService,
            requestScopedDependencies,
            algorithmComputer
        );

        var configuration = new ExampleConfiguration();
        var graphResources = new GraphResources(null, mock(Graph.class), null);
        when(graphStoreCatalogService.getGraphResources(
            GraphName.parse("some other graph"),
            configuration,
            Optional.empty(),
            Optional.empty(),
            requestScopedDependencies.getUser(),
            requestScopedDependencies.getDatabaseId()
        )).thenReturn(graphResources);
        when(algorithmComputer.computeAlgorithm(
            configuration,
            graphResources,
            Dijkstra,
            null,
            null
        )).thenReturn("some other result");
        Object renderedResult = template.processAlgorithmAndAnySideEffects(
            Optional.empty(),
            GraphName.parse("some other graph"),
            configuration,
            Optional.empty(),
            Dijkstra,
            null,
            null,
            Optional.empty(),
            (__, result, timings, metadata) -> {
                assertThat(result).hasValue("some other result");
                assertThat(timings.preProcessingMillis).isGreaterThan(-1);
                assertThat(timings.computeMillis).isGreaterThan(-1);
                assertThat(timings.mutateOrWriteMillis).isEqualTo(-1); // no side effect, no timing
                assertThat(metadata).isEmpty();

                return "some other rendered result";
            }
        );

        assertThat(renderedResult).isEqualTo("some other rendered result");
    }
}
