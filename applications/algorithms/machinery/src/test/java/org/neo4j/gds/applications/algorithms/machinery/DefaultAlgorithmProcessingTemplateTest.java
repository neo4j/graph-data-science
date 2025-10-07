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
import org.neo4j.gds.core.RequestCorrelationId;
import org.neo4j.gds.core.loading.GraphResources;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.logging.Log;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Pinky promise: next time we add or change functionality here,
 * we break this class apart so that we can test it more sensibly. Smaller chunks, more readable class,
 * and tests that are not pages long.
 */
class DefaultAlgorithmProcessingTemplateTest {
    @Test
    void shouldDoFourStepProcess() {
        var log = mock(Log.class);
        var graphStoreCatalogService = mock(GraphStoreCatalogService.class);
        var requestScopedDependencies = RequestScopedDependencies.builder().correlationId(new RequestCorrelationId() {
            @Override
            public String toString() {
                return "my little gid";
            }
        }).build();
        var algorithmComputer = mock(ComputationService.class);
        var template = new DefaultAlgorithmProcessingTemplate(
            log,
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
            Optional.empty(),
            requestScopedDependencies.user(),
            requestScopedDependencies.databaseId()
        )).thenReturn(graphResources);
        when(algorithmComputer.computeAlgorithm(
            configuration,
            graphResources,
            new StandardLabel("some compute job"),
            null,
            null,
            DimensionTransformer.DISABLED
        )).thenReturn("some result");
        Object renderedResult = template.processAlgorithmAndAnySideEffects(
            Optional.empty(),
            GraphName.parse("some graph"),
            configuration,
            Optional.empty(),
            Optional.empty(),
            new StandardLabel("some compute job"),
            DimensionTransformer.DISABLED,
            null,
            null,
            Optional.of((__, ___) -> Optional.of("metadata from some side effect")),
            (__, result, timings, metadata) -> {
                assertThat(result).hasValue("some result");
                assertThat(timings.preProcessingMillis).isGreaterThan(-1);
                assertThat(timings.computeMillis).isGreaterThan(-1);
                assertThat(timings.sideEffectMillis).isGreaterThan(-1);
                assertThat(metadata).hasValue("metadata from some side effect");

                return "some rendered result";
            }
        );

        assertThat(renderedResult).isEqualTo("some rendered result");

        verify(log).info("[my little gid] Algorithm processing commencing");
        verify(log).info("[my little gid] Computing algorithm");
        verify(log).info("[my little gid] Processing algorithm result");
        verify(log).info("[my little gid] Rendering output");
        verify(log).info("[my little gid] Algorithm processing complete");
    }

    @Test
    void shouldSkipSideEffect() {
        var log = mock(Log.class);
        var graphStoreCatalogService = mock(GraphStoreCatalogService.class);
        var requestScopedDependencies = RequestScopedDependencies.builder().correlationId(new RequestCorrelationId() {
            @Override
            public String toString() {
                return "job's a good 'un";
            }
        }).build();
        var algorithmComputer = mock(ComputationService.class);
        var template = new DefaultAlgorithmProcessingTemplate(
            log,
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
            Optional.empty(),
            requestScopedDependencies.user(),
            requestScopedDependencies.databaseId()
        )).thenReturn(graphResources);
        when(algorithmComputer.computeAlgorithm(
            configuration,
            graphResources,
            new StandardLabel("some other compute job"),
            null,
            null,
            DimensionTransformer.DISABLED
        )).thenReturn("some other result");
        Object renderedResult = template.processAlgorithmAndAnySideEffects(
            Optional.empty(),
            GraphName.parse("some other graph"),
            configuration,
            Optional.empty(),
            Optional.empty(),
            new StandardLabel("some other compute job"),
            DimensionTransformer.DISABLED,
            null,
            null,
            Optional.empty(),
            (__, result, timings, metadata) -> {
                assertThat(result).hasValue("some other result");
                assertThat(timings.preProcessingMillis).isGreaterThan(-1);
                assertThat(timings.computeMillis).isGreaterThan(-1);
                assertThat(timings.sideEffectMillis).isEqualTo(-1); // no side effect, no timing
                assertThat(metadata).isEmpty();

                return "some other rendered result";
            }
        );

        assertThat(renderedResult).isEqualTo("some other rendered result");

        verify(log).info("[job's a good 'un] Algorithm processing commencing");
        verify(log).info("[job's a good 'un] Computing algorithm");
        verify(log).info("[job's a good 'un] Processing algorithm result");
        verify(log).info("[job's a good 'un] Rendering output");
        verify(log).info("[job's a good 'un] Algorithm processing complete");
    }

    @Test
    void shouldLogProcessingEvenWhenErrorsHappen() {
        var log = mock(Log.class);
        var graphStoreCatalogService = mock(GraphStoreCatalogService.class);
        var requestScopedDependencies = RequestScopedDependencies.builder().correlationId(new RequestCorrelationId() {
            @Override
            public String toString() {
                return "opaque job ids ftw";
            }
        }).build();
        var algorithmComputer = mock(ComputationService.class);
        var template = new DefaultAlgorithmProcessingTemplate(
            log,
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
            Optional.empty(),
            requestScopedDependencies.user(),
            requestScopedDependencies.databaseId()
        )).thenReturn(graphResources);
        when(algorithmComputer.computeAlgorithm(
            configuration,
            graphResources,
            new StandardLabel("some other compute job"),
            null,
            null,
            DimensionTransformer.DISABLED
        )).thenReturn("some other result");

        try {
            template.processAlgorithmAndAnySideEffects(
                Optional.empty(),
                GraphName.parse("some other graph"),
                configuration,
                Optional.empty(),
                Optional.empty(),
                new StandardLabel("some other compute job"),
                DimensionTransformer.DISABLED,
                null,
                null,
                Optional.of((__, ___) -> {
                    try {
                        throw new RuntimeException("Fly, you fools!");
                    } finally {
                        log.info("[opaque job ids ftw] Error: wouldn't it be nice if it behaved like this");
                    }
                }),
                null
            );

            fail();
        } catch (RuntimeException e) {
            assertEquals("Fly, you fools!", e.getMessage());
        }

        verify(log).info("[opaque job ids ftw] Algorithm processing commencing");
        verify(log).info("[opaque job ids ftw] Computing algorithm");
        verify(log).info("[opaque job ids ftw] Processing algorithm result");
        verify(log).info("[opaque job ids ftw] Error: wouldn't it be nice if it behaved like this");
        // it would be nice, actually, but this is just logging, so bits of our code can opt in,
        // but we cannot mandate nor guarantee
        verify(log, never()).info("[opaque job ids ftw] Rendering output"); // because exception happened
        verify(log).info("[opaque job ids ftw] Algorithm processing complete"); // but you get the finaliser
    }
}
