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
package org.neo4j.gds.algorithms.community;

    import org.junit.jupiter.api.Test;
    import org.neo4j.gds.algorithms.AlgorithmComputationResult;
    import org.neo4j.gds.algorithms.community.specificfields.StandardCommunityStatisticsSpecificFields;
    import org.neo4j.gds.algorithms.writeservices.WriteNodePropertyResult;
    import org.neo4j.gds.algorithms.writeservices.WriteNodePropertyService;
    import org.neo4j.gds.api.Graph;
    import org.neo4j.gds.api.GraphStore;
    import org.neo4j.gds.api.properties.nodes.NodePropertyValuesAdapter;
    import org.neo4j.gds.collections.ha.HugeLongArray;
    import org.neo4j.gds.config.AlgoBaseConfig;
    import org.neo4j.gds.core.concurrency.Concurrency;
    import org.neo4j.gds.result.StatisticsComputationInstructions;
    import org.neo4j.gds.wcc.WccWriteConfig;

    import java.util.Optional;

    import static org.assertj.core.api.Assertions.assertThat;
    import static org.mockito.ArgumentMatchers.any;
    import static org.mockito.ArgumentMatchers.eq;
    import static org.mockito.Mockito.mock;
    import static org.mockito.Mockito.verifyNoInteractions;
    import static org.mockito.Mockito.when;

    class CommunityAlgorithmsWriteBusinessFacadeTest {

        @Test
    void writeWithoutAlgorithmResult() {

        var configurationMock = mock(WccWriteConfig.class);
        var graph=mock(Graph.class);
        var graphStore = mock(GraphStore.class);
        var algorithmResult = AlgorithmComputationResult.<Long>withoutAlgorithmResult(graph, graphStore);

        var nodePropertyServiceMock = mock(WriteNodePropertyService.class);

            var businessFacade = new CommunityAlgorithmsWriteBusinessFacade(nodePropertyServiceMock, null);

        var writeResult = businessFacade.writeToDatabase(
            algorithmResult,
            configurationMock,
            null,
            null,
            0L,
            () -> 19L,
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

        assertThat(writeResult.algorithmSpecificFields())
            .as("Incorrect additional algorithm field value")
            .isEqualTo(19L);
    }

        @Test
        void writeWithoutCommunityStatistics() {

            var configurationMock = mock(WccWriteConfig.class);
            var graph = mock(Graph.class);
            var graphStore = mock(GraphStore.class);


            var algoResult = HugeLongArray.of(10, 12, 12, 13);

            var algorithmResultMock = AlgorithmComputationResult.of(
                algoResult,
                graph,
                graphStore
            );

            when(graph.nodeCount()).thenReturn(4l);

            var nodePropertyServiceMock = mock(WriteNodePropertyService.class);

            NodePropertyValuesMapper<HugeLongArray, AlgoBaseConfig> nodePropertyValuesMapper =
                (r, c) -> NodePropertyValuesAdapter.adapt(r);

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

            var businessFacade = new CommunityAlgorithmsWriteBusinessFacade(nodePropertyServiceMock, null);

            var writeResult = businessFacade.writeToDatabase(
                algorithmResultMock,
                configurationMock,
                nodePropertyValuesMapper,
                result -> 22L,
                0L,
                () -> 19L,
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

            assertThat(writeResult.algorithmSpecificFields())
                .as("correct additional algorithm field value")
                .isEqualTo(22);
        }

        @Test
        void writeWithCommunityStatistics() {

            var configurationMock = mock(WccWriteConfig.class);
            when(configurationMock.typedConcurrency()).thenReturn(new Concurrency(4));
            var graph = mock(Graph.class);
            var graphStore = mock(GraphStore.class);


            var algoResult = HugeLongArray.of(10, 12, 12, 13);

            var algorithmResultMock = AlgorithmComputationResult.of(
                algoResult,
                graph,
                graphStore
            );

            when(graph.nodeCount()).thenReturn(4l);

            var statisticsComputationInstructionsMock = mock(StatisticsComputationInstructions.class);
            when(statisticsComputationInstructionsMock.computeCountOnly()).thenReturn(true);

            var nodePropertyServiceMock = mock(WriteNodePropertyService.class);

            NodePropertyValuesMapper<HugeLongArray, AlgoBaseConfig> nodePropertyValuesMapper =
                (r, c) -> NodePropertyValuesAdapter.adapt(r);

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

            var businessFacade = new CommunityAlgorithmsWriteBusinessFacade(nodePropertyServiceMock, null);

            var writeResult = businessFacade.writeToDatabase(
                algorithmResultMock,
                configurationMock,
                nodePropertyValuesMapper,
                (result) -> algoResult::get,
                (r, cc, cs) -> new StandardCommunityStatisticsSpecificFields(
                    cc,
                    cs
                ),
                statisticsComputationInstructionsMock,
                0L,
                () -> StandardCommunityStatisticsSpecificFields.EMPTY,
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

            assertThat(writeResult.algorithmSpecificFields().communityCount())
                .as("correct  community number")
                .isEqualTo(3);

        }

    }
