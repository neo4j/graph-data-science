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
package org.neo4j.gds.algorithms.runner;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.Preconditions;
import org.neo4j.gds.PreconditionsProvider;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.User;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.loading.GraphResources;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.loading.PostLoadValidationHook;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.metrics.ExecutionMetric;
import org.neo4j.gds.metrics.algorithms.AlgorithmMetricsService;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThatException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class AlgorithmRunnerTest {
    /**
     * Save the preconditions before each test, restore them after
     * That way, there will be no global effects - fingers crossed
     * Now, the fact that there _could_ be global effects is just hella scary
     * Luckily I am fixing that, one little mess after another
     */
    private Preconditions preconditions;

    @BeforeEach
    public void before() {
        preconditions = PreconditionsProvider.preconditions();
    }

    @AfterEach
    public void after() {
        PreconditionsProvider.preconditions(preconditions);
    }

    @Test
    void shouldRegisterAlgorithmMetricCountForSuccess() {
        var algorithmMetricMock = mock(ExecutionMetric.class);
        var algorithmMetricsServiceMock = mock(AlgorithmMetricsService.class);
        when(algorithmMetricsServiceMock.create(anyString())).thenReturn(algorithmMetricMock);

        var runner = new AlgorithmRunner(
            null,
            null,
            algorithmMetricsServiceMock,
            null,
            null
        );

        var algorithmMock = mock(Algorithm.class);
        when(algorithmMock.compute()).thenReturn("WooHoo");


        runner.runAlgorithm(algorithmMock, "TestingMetrics");

        verify(algorithmMetricsServiceMock, times(1)).create("TestingMetrics");
        verify(algorithmMetricMock, times(1)).start();
        verify(algorithmMetricMock, times(1)).close();
        verify(algorithmMetricMock, times(0)).failed(any());
        verifyNoMoreInteractions(
            algorithmMetricsServiceMock,
            algorithmMetricMock
        );
    }


    @Test
    void shouldRegisterAlgorithmMetricCountForFailure() {
        var algorithmMetricMock = mock(ExecutionMetric.class);
        var algorithmMetricsServiceMock = mock(AlgorithmMetricsService.class);
        when(algorithmMetricsServiceMock.create(anyString())).thenReturn(algorithmMetricMock);

        var logMock = mock(Log.class);
        when(logMock.getNeo4jLog()).thenReturn(Neo4jProxy.testLog());

        var runner = new AlgorithmRunner(
            logMock,
            null,
            algorithmMetricsServiceMock,
            null,
            null
        );

        var algorithmMock = mock(Algorithm.class, RETURNS_DEEP_STUBS);
        when(algorithmMock.compute()).thenThrow(new RuntimeException("Ooops"));

        assertThatException().isThrownBy(
            () -> runner.runAlgorithm(
                algorithmMock,
                "TestingMetrics"
            )
        ).withMessage("Ooops");

        verify(algorithmMetricsServiceMock, times(1)).create("TestingMetrics");
        verify(algorithmMetricMock, times(1)).start();
        verify(algorithmMetricMock, times(1)).close();
        verify(algorithmMetricMock, times(1)).failed(any());
        verifyNoMoreInteractions(
            algorithmMetricsServiceMock,
            algorithmMetricMock
        );
    }

    @Test
    void shouldFailWhenPreconditionsAreNotMet() {
        PreconditionsProvider.preconditions(() -> {
            throw new IllegalStateException("Here be dragons");
        });
        var algorithmRunner = new AlgorithmRunner(null, null, null, null, null);

        assertThatThrownBy(() -> algorithmRunner.run(null, null, null, null)).hasMessageContaining("Here be dragons");
    }

    @Test
    void shouldSucceedWhenPreconditionsAreMet() {
        PreconditionsProvider.preconditions(() -> {});
        var graphStoreCatalogService = new GraphStoreCatalogService() {
            @Override
            public GraphResources getGraphResources(
                GraphName graphName,
                AlgoBaseConfig configuration,
                Optional<Iterable<PostLoadValidationHook>> postGraphStoreLoadValidationHooks,
                Optional<String> relationshipProperty,
                User user,
                DatabaseId databaseId
            ) {
                // this gets called right after the preconditions check
                // so, we save ourselves trouble and shunt the rest of the method
                throw new RuntimeException("just a shortcut");
            }
        };
        var algorithmRunner = new AlgorithmRunner(
            null,
            graphStoreCatalogService,
            null,
            null,
            RequestScopedDependencies.builder().build()
        );

        assertThatThrownBy(() -> algorithmRunner.run("doesn't matter", null, null, null))
            .hasMessageContaining("just a shortcut");
    }
}
