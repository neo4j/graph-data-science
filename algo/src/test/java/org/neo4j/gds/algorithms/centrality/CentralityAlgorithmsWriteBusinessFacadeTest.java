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
    import org.neo4j.gds.algorithms.centrality.specificfields.StandardCentralityStatisticsSpecificFields;
    import org.neo4j.gds.algorithms.writeservices.WriteNodePropertyResult;
    import org.neo4j.gds.algorithms.writeservices.WriteNodePropertyService;
    import org.neo4j.gds.api.Graph;
    import org.neo4j.gds.api.GraphStore;
    import org.neo4j.gds.api.properties.nodes.NodePropertyValuesAdapter;
    import org.neo4j.gds.betweenness.BetweennessCentralityWriteConfig;
    import org.neo4j.gds.collections.ha.HugeDoubleArray;
    import org.neo4j.gds.core.utils.TerminationFlag;

    import java.util.Optional;

    import static org.assertj.core.api.Assertions.assertThat;
    import static org.mockito.ArgumentMatchers.any;
    import static org.mockito.ArgumentMatchers.eq;
    import static org.mockito.Mockito.mock;
    import static org.mockito.Mockito.verifyNoInteractions;
    import static org.mockito.Mockito.when;

    class CentralityAlgorithmsWriteBusinessFacadeTest {

        @Test
    void writeWithoutAlgorithmResult() {

        var configurationMock = mock(BetweennessCentralityWriteConfig.class);
        var graph=mock(Graph.class);
        var graphStore = mock(GraphStore.class);
        var algorithmResult = AlgorithmComputationResult.<Long>withoutAlgorithmResult(graph, graphStore);

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
            ()-> StandardCentralityStatisticsSpecificFields.EMPTY,
            "FOO",
            4,
            "foo",
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
                TerminationFlag.RUNNING_TRUE
            );

            when(graph.nodeCount()).thenReturn(4l);



            var nodePropertyServiceMock = mock(WriteNodePropertyService.class);
            NodePropertyValuesMapper<HugeDoubleArray> nodePropertyValuesMapper = (r)-> NodePropertyValuesAdapter.adapt(r);


            when(nodePropertyServiceMock.write(
                eq(graph),
                eq(graphStore),
                any(),
                eq(4),
                eq("foo"),
                eq("FooWrite"),
                eq(Optional.empty()),
                eq(TerminationFlag.RUNNING_TRUE)
            )).thenReturn(new WriteNodePropertyResult(4, 100));

            var businessFacade = new CentralityAlgorithmsWriteBusinessFacade(null, nodePropertyServiceMock);

            var writeResult = businessFacade.writeToDatabase(
                algorithmResultMock,
                configurationMock,
                ((r)->r::get),
                nodePropertyValuesMapper,
                ((r,  cs) -> new StandardCentralityStatisticsSpecificFields(cs)),
                false,
                0L,
                () -> StandardCentralityStatisticsSpecificFields.EMPTY,
                "FooWrite",
                4,
                "foo",
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
            var graph = mock(Graph.class);
            var graphStore = mock(GraphStore.class);

            var result = HugeDoubleArray.of(0.1, 0.2, 0.3, 0.4);
            var algorithmResultMock = AlgorithmComputationResult.of(
                result,
                graph,
                graphStore,
                TerminationFlag.RUNNING_TRUE
            );

            when(graph.nodeCount()).thenReturn(4l);



            var nodePropertyServiceMock = mock(WriteNodePropertyService.class);
            NodePropertyValuesMapper<HugeDoubleArray> nodePropertyValuesMapper = (r)-> NodePropertyValuesAdapter.adapt(r);


            when(nodePropertyServiceMock.write(
                eq(graph),
                eq(graphStore),
                any(),
                eq(4),
                eq("foo"),
                eq("FooWrite"),
                eq(Optional.empty()),
                eq(TerminationFlag.RUNNING_TRUE)
            )).thenReturn(new WriteNodePropertyResult(4, 100));

            var businessFacade = new CentralityAlgorithmsWriteBusinessFacade(null, nodePropertyServiceMock);

            var writeResult = businessFacade.writeToDatabase(
                algorithmResultMock,
                configurationMock,
                ((r)->r::get),
                nodePropertyValuesMapper,
                ((r,  cs) -> new StandardCentralityStatisticsSpecificFields(cs)),
                true,
                0L,
                () -> StandardCentralityStatisticsSpecificFields.EMPTY,
                "FooWrite",
                4,
                "foo",
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

            assertThat((double)writeResult.algorithmSpecificFields().centralityDistribution().get("mean"))
                .as("correct  mean")
                .isCloseTo(0.25, Offset.offset(1e-4));

        }
        
    }
