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
package org.neo4j.gds.algorithms.centrality;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.algorithms.AlgorithmComputationResult;
import org.neo4j.gds.algorithms.centrality.specificfields.DefaultCentralitySpecificFields;
import org.neo4j.gds.algorithms.writeservices.WriteNodePropertyResult;
import org.neo4j.gds.algorithms.writeservices.WriteNodePropertyService;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodePropertyValuesAdapter;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.betweenness.BetweennessCentralityWriteConfig;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.config.ArrowConnectionInfo;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.pagerank.PageRankResult;
import org.neo4j.gds.pagerank.PageRankWriteConfig;
import org.neo4j.gds.scaling.ScalerFactory;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class CentralityAlgorithmsWriteBusinessFacadeTest {

    @Test
    void writeWithoutAlgorithmResult() {

        var configurationMock = mock(BetweennessCentralityWriteConfig.class);
        var graph = mock(Graph.class);
        var graphStore = mock(GraphStore.class);
        var algorithmResult = AlgorithmComputationResult.<Long>withoutAlgorithmResult(graph, graphStore, ResultStore.EMPTY);

        var nodePropertyServiceMock = mock(WriteNodePropertyService.class);

        var businessFacade = new CentralityAlgorithmsWriteBusinessFacade(null, nodePropertyServiceMock);

        var writeResult = businessFacade.writeToDatabase(
            algorithmResult,
            configurationMock,
            null,
            null,
            null,
            true,
            0,
            () -> DefaultCentralitySpecificFields.EMPTY,
            "FOO",
            new Concurrency(4),
            "foo",
            Optional.empty(),
            Optional.empty()
        );

        verifyNoInteractions(nodePropertyServiceMock);

        assertThat(writeResult.configuration())
            .as("The configuration should be the exact same object")
            .isSameAs(configurationMock);

        assertThat(writeResult.nodePropertiesWritten())
            .as("no properties should be written")
            .isSameAs(0L);

        assertThat(writeResult.algorithmSpecificFields().centralityDistribution())
            .as(" empty map")
            .isEmpty();
    }

    @Test
    void writeWithoutStatistics() {

        var configurationMock = mock(BetweennessCentralityWriteConfig.class);
        var graph = mock(Graph.class);
        var graphStore = mock(GraphStore.class);

        var result = HugeDoubleArray.of(0.1, 0.2, 0.3, 0.4);
        var algorithmResultMock = AlgorithmComputationResult.of(
            result,
            graph,
            graphStore,
            ResultStore.EMPTY
        );

        when(graph.nodeCount()).thenReturn(4L);


        var nodePropertyServiceMock = mock(WriteNodePropertyService.class);
        NodePropertyValuesMapper<HugeDoubleArray> nodePropertyValuesMapper = (r) -> NodePropertyValuesAdapter.adapt(r);


        when(nodePropertyServiceMock.write(
            eq(graph),
            eq(graphStore),
            any(),
            eq(new Concurrency(4)),
            eq("foo"),
            eq("FooWrite"),
            eq(Optional.empty()),
            eq(Optional.empty())
        )).thenReturn(new WriteNodePropertyResult(4, 100));

        var businessFacade = new CentralityAlgorithmsWriteBusinessFacade(null, nodePropertyServiceMock);

        var writeResult = businessFacade.writeToDatabase(
            algorithmResultMock,
            configurationMock,
            ((r) -> r::get),
            nodePropertyValuesMapper,
            ((r, cs) -> new DefaultCentralitySpecificFields(cs)),
            false,
            0L,
            () -> DefaultCentralitySpecificFields.EMPTY,
            "FooWrite",
            new Concurrency(4),
            "foo",
            Optional.empty(),
            Optional.empty()
        );


        assertThat(writeResult.configuration())
            .as("The configuration should be the exact same object")
            .isSameAs(configurationMock);

        assertThat(writeResult.nodePropertiesWritten())
            .as("no properties should be written")
            .isSameAs(4L);

        assertThat(writeResult.writeMillis())
            .as("correct  writeMillis")
            .isEqualTo(100);

        assertThat(writeResult.algorithmSpecificFields().centralityDistribution())
            .as("empty map")
            .isEmpty();
    }

    @Test
    void writeWithStatistics() {

        var configurationMock = mock(BetweennessCentralityWriteConfig.class);
        when(configurationMock.typedConcurrency()).thenReturn(new Concurrency(4));
        var graph = mock(Graph.class);
        var graphStore = mock(GraphStore.class);

        var result = HugeDoubleArray.of(0.1, 0.2, 0.3, 0.4);
        var algorithmResultMock = AlgorithmComputationResult.of(
            result,
            graph,
            graphStore,
            ResultStore.EMPTY
        );

        when(graph.nodeCount()).thenReturn(4l);


        var nodePropertyServiceMock = mock(WriteNodePropertyService.class);
        NodePropertyValuesMapper<HugeDoubleArray> nodePropertyValuesMapper = (r) -> NodePropertyValuesAdapter.adapt(r);


        when(nodePropertyServiceMock.write(
            eq(graph),
            eq(graphStore),
            any(),
            eq(new Concurrency(4)),
            eq("foo"),
            eq("FooWrite"),
            eq(Optional.empty()),
            eq(Optional.empty())
        )).thenReturn(new WriteNodePropertyResult(4, 100));

        var businessFacade = new CentralityAlgorithmsWriteBusinessFacade(null, nodePropertyServiceMock);

        var writeResult = businessFacade.writeToDatabase(
            algorithmResultMock,
            configurationMock,
            ((r) -> r::get),
            nodePropertyValuesMapper,
            ((r, cs) -> new DefaultCentralitySpecificFields(cs)),
            true,
            0L,
            () -> DefaultCentralitySpecificFields.EMPTY,
            "FooWrite",
            new Concurrency(4),
            "foo",
            Optional.empty(),
            Optional.empty()
        );


        assertThat(writeResult.configuration())
            .as("The configuration should be the exact same object")
            .isSameAs(configurationMock);

        assertThat(writeResult.nodePropertiesWritten())
            .as("no properties should be written")
            .isSameAs(4L);

        assertThat(writeResult.writeMillis())
            .as("correct  writeMillis")
            .isEqualTo(100);

        assertThat((double) writeResult.algorithmSpecificFields().centralityDistribution().get("mean"))
            .as("correct  mean")
            .isCloseTo(0.25, Offset.offset(1e-4));

    }

    @Test
    void pageRankShouldHaveErrorHintDistributionForLogScaler() {
        var pageRankConfigMock = mock(PageRankWriteConfig.class);
        when(pageRankConfigMock.scaler()).thenReturn(ScalerFactory.parse("log"));

        var centralityAlgorithmsFacadeMock = mock(CentralityAlgorithmsFacade.class);
        var pageRankResultMock = mock(PageRankResult.class);

        when(centralityAlgorithmsFacadeMock.pageRank(anyString(), any()))
            .thenReturn(
                AlgorithmComputationResult.of(
                    pageRankResultMock,
                    mock(Graph.class),
                    mock(GraphStore.class),
                    ResultStore.EMPTY
                )
            );

        var nodePropertyServiceStub = new WriteNodePropertyServiceStub(4, 100);

        var businessFacade = new CentralityAlgorithmsWriteBusinessFacade(
            centralityAlgorithmsFacadeMock,
            nodePropertyServiceStub
        );
        var pageRankResult = businessFacade.pageRank("meh", pageRankConfigMock, true);

        assertThat(pageRankResult.algorithmSpecificFields().centralityDistribution())
            .hasSize(1)
            .containsEntry("Error", "Unable to create histogram when using scaler of type LOG");
    }

    @Test
    void pageRankShouldHaveRealDistributionWhenScalerIsNotLog() {
        var pageRankConfigMock = mock(PageRankWriteConfig.class);
        when(pageRankConfigMock.scaler()).thenReturn(ScalerFactory.parse("none"));
        when(pageRankConfigMock.typedConcurrency()).thenReturn(new Concurrency(4));

        var centralityAlgorithmsFacadeMock = mock(CentralityAlgorithmsFacade.class);
        var pageRankResultMock = mock(PageRankResult.class);
        when(pageRankResultMock.nodeCount()).thenReturn(190L);
        when(pageRankResultMock.centralityScoreProvider()).thenReturn(n -> (double) n);

        when(centralityAlgorithmsFacadeMock.pageRank(anyString(), any()))
            .thenReturn(
                AlgorithmComputationResult.of(
                    pageRankResultMock,
                    mock(Graph.class),
                    mock(GraphStore.class),
                    ResultStore.EMPTY
                )
            );

        var nodePropertyServiceStub = new WriteNodePropertyServiceStub(4, 100);

        var businessFacade = new CentralityAlgorithmsWriteBusinessFacade(
            centralityAlgorithmsFacadeMock,
            nodePropertyServiceStub
        );
        var pageRankResult = businessFacade.pageRank("meh", pageRankConfigMock, true);

        assertThat(pageRankResult.algorithmSpecificFields().centralityDistribution())
            .hasSize(9)
            .containsKeys("max", "mean", "min", "p50", "p75", "p90", "p95", "p99", "p999")
            .doesNotContainKey("Error");

    }

    private static final class WriteNodePropertyServiceStub extends WriteNodePropertyService {
        private final long nodePropertiesWritten;
        private final long writeMilliseconds;

        WriteNodePropertyServiceStub(long nodePropertiesWritten, long writeMilliseconds) {
            super(null, RequestScopedDependencies.builder().build());
            this.nodePropertiesWritten = nodePropertiesWritten;
            this.writeMilliseconds = writeMilliseconds;
        }

        @Override
        public WriteNodePropertyResult write(
            Graph graph,
            GraphStore graphStore,
            NodePropertyValues nodePropertyValues,
            Concurrency writeConcurrency,
            String writeProperty,
            String procedureName,
            Optional<ArrowConnectionInfo> arrowConnectionInfo,
            Optional<ResultStore> resultStore
        ) {
            return new WriteNodePropertyResult(nodePropertiesWritten, writeMilliseconds);
        }
    }
}
