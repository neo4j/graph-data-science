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
package org.neo4j.gds.maxflow;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.InputNodes;
import org.neo4j.gds.MapInputNodes;
import org.neo4j.gds.api.properties.nodes.DoubleNodePropertyValues;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@GdlExtension
@Nested
class MaxFlowWithNodeConstraintsTest {

    @GdlExtension
    @Nested
    class SimpleLine{
        @GdlGraph
        private static final String GRAPH =
            """
                CREATE
                    (a:Node {id: 0}),
                    (b:Node {id: 1}),
                    (c:Node {id: 2}),
                    (d:Node {id: 3}),
                    (e:Node {id: 4}),
                    (f:Node {id: 5}),
                    (a)-[:R {w: 100.0}]->(b),
                    (b)-[:R {w: 100.0}]->(c),
                    (c)-[:R {w: 100.0}]->(d),
                    (d)-[:R {w: 100.0}]->(e),
                    (e)-[:R {w: 100.0}]->(f)
                """;

        @Inject
        private TestGraph graph;

        @Test
        void shouldFindTheCorrectFlow(){
            //b has capacity 10 //c has capacity 6 //d has capacity 2// e has capacity 4

            var capacities = Map.of(
                graph.toMappedNodeId("b"),10d,
                graph.toMappedNodeId("c"),6d,
                graph.toMappedNodeId("d"),2d,
                graph.toMappedNodeId("e"),4d
            );

            var nodeProps = new DoubleNodePropertyValues(){
                @Override
                public double doubleValue(long nodeId) {
                    return capacities.getOrDefault(nodeId,Double.NaN);
                }

                @Override
                public long nodeCount() {
                    return graph.nodeCount();
                }
            };

            InputNodes sources = new MapInputNodes(Map.of(graph.toMappedNodeId("a"),100d));
            InputNodes sinks = new MapInputNodes(Map.of(graph.toMappedNodeId("f"),100d));

            NodeWithValue[] supply =new NodeWithValue[]{new NodeWithValue(graph.toMappedNodeId("a"),100)};
            NodeWithValue[] demand =new NodeWithValue[]{new NodeWithValue(graph.toMappedNodeId("f"),100)};


            var nodeConstraints = NodeConstraintsFromPropertyIdMap.create(
                graph,
                graph.relationshipCount(),
                nodeProps,
                sources,
                sinks
            );

            var flowGraph = new FlowGraphBuilder(
                graph,
                supply,
                demand,
                TerminationFlag.RUNNING_TRUE,
                new Concurrency(1),
                nodeConstraints
            ).build();

             var maxFlowParams  = mock(MaxFlowParameters.class);
             when(maxFlowParams.concurrency()).thenReturn(new Concurrency(1));
             when(maxFlowParams.useGapRelabelling()).thenReturn(true);
             when(maxFlowParams.freq()).thenReturn(0.5);
             var excess = HugeDoubleArray.newArray(flowGraph.nodeCount());
             
            var maxFlow = new MaxFlowPhase(flowGraph,excess,maxFlowParams, ProgressTracker.NULL_TRACKER,TerminationFlag.RUNNING_TRUE);
             maxFlow.computeMaxFlow();

             var flowResult = flowGraph.createFlowResult();
             assertThat(flowResult.totalFlow()).isEqualTo(2);
             var expectedRelationships = Set.of(
                 new FlowRelationship(graph.toMappedNodeId("a"),graph.toMappedNodeId("b"),2),
                 new FlowRelationship(graph.toMappedNodeId("b"),graph.toMappedNodeId("c"),2),
                 new FlowRelationship(graph.toMappedNodeId("c"),graph.toMappedNodeId("d"),2),
                 new FlowRelationship(graph.toMappedNodeId("d"),graph.toMappedNodeId("e"),2),
                 new FlowRelationship(graph.toMappedNodeId("e"),graph.toMappedNodeId("f"),2)
             );
             assertThat(flowResult.flow().toArray()).containsExactlyInAnyOrderElementsOf(expectedRelationships);
        }
    }

    @GdlExtension
    @Nested
    class DagGraph {
        @GdlGraph
        private static final String GRAPH =
            """
                CREATE
                    (a:Node {id: 0}),
                    (b:Node {id: 1, cap:5}),
                    (c:Node {id: 2}),
                    (d:Node {id: 3}),
                    (e:Node {id: 4, cap:2}),
                    (f:Node {id: 5}),
                    (a)-[:R {w: 100.0}]->(b),
                    (a)-[:R {w: 8.0}]->(c),
                    (b)-[:R {w: 10.0}]->(d),
                    (c)-[:R {w: 6.0}]->(e),
                    (b)-[:R {w: 1.0}]->(f),
                    (c)-[:R {w: 6.0}]->(f)
                    (d)-[:R {w: 10.0}]->(f),
                    (e)-[:R {w: 100.0}]->(f)
                """;

        @Inject
        private TestGraph graph;

        @Test
        void shouldFindTheCorrectFlow(){

            var nodeProps = graph.nodeProperties("cap");

            NodeWithValue[] supply =new NodeWithValue[]{new NodeWithValue(graph.toMappedNodeId("a"),100)};
            NodeWithValue[] demand =new NodeWithValue[]{new NodeWithValue(graph.toMappedNodeId("f"),100)};

            InputNodes sources = new MapInputNodes(Map.of(graph.toMappedNodeId("a"),100d));
            InputNodes sinks = new MapInputNodes(Map.of(graph.toMappedNodeId("f"),100d));

            var nodeConstraints = NodeConstraintsFromPropertyIdMap.create(
                graph,
                graph.relationshipCount(),
                nodeProps,
                sources,
                sinks
            );

            var flowGraph = new FlowGraphBuilder(
                graph,
                supply,
                demand,
                TerminationFlag.RUNNING_TRUE,
                new Concurrency(1),
                nodeConstraints
            ).build();

            var maxFlowParams  = mock(MaxFlowParameters.class);
            when(maxFlowParams.concurrency()).thenReturn(new Concurrency(1));
            when(maxFlowParams.useGapRelabelling()).thenReturn(true);
            when(maxFlowParams.freq()).thenReturn(0.5);
            var excess = HugeDoubleArray.newArray(flowGraph.nodeCount());

            var maxFlow = new MaxFlowPhase(flowGraph,excess,maxFlowParams, ProgressTracker.NULL_TRACKER,TerminationFlag.RUNNING_TRUE);
            maxFlow.computeMaxFlow();

            var flowResult = flowGraph.createFlowResult();
            assertThat(flowResult.totalFlow()).isEqualTo(13);
            var expectedRelationships = Set.of(
                new FlowRelationship(graph.toMappedNodeId("a"),graph.toMappedNodeId("b"),5),
                new FlowRelationship(graph.toMappedNodeId("a"),graph.toMappedNodeId("c"),8),
                new FlowRelationship(graph.toMappedNodeId("b"),graph.toMappedNodeId("f"),1),
                new FlowRelationship(graph.toMappedNodeId("b"),graph.toMappedNodeId("d"),4),
                new FlowRelationship(graph.toMappedNodeId("d"),graph.toMappedNodeId("f"),4),
                 new FlowRelationship(graph.toMappedNodeId("c"),graph.toMappedNodeId("f"),6),
                new FlowRelationship(graph.toMappedNodeId("c"),graph.toMappedNodeId("e"),2),
                new FlowRelationship(graph.toMappedNodeId("e"),graph.toMappedNodeId("f"),2)
            );
            assertThat(flowResult.flow().toArray()).containsExactlyInAnyOrderElementsOf(expectedRelationships);
        }
        }

    }
