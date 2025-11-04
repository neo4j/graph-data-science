
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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.ListInputNodes;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.beta.generator.PropertyProducer;
import org.neo4j.gds.beta.generator.RandomGraphGenerator;
import org.neo4j.gds.beta.generator.RelationshipDistribution;
import org.neo4j.gds.config.RandomGraphGeneratorConfig;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.TestSupport.fromGdl;

class MaxFlow2Test {
    private static final double TOLERANCE = 1e-6;


    class RandomLongsAsDoublesProducer implements PropertyProducer<double[]> {
        private final String propertyName;
        private final long min;
        private final long max;

        public RandomLongsAsDoublesProducer(String propertyName, long min, long max) {
            this.propertyName = propertyName;
            this.min = min;
            this.max = max;


        }

        @Override
        public String getPropertyName() {
            return propertyName;
        }

        @Override
        public ValueType propertyType() {
            return ValueType.DOUBLE;
        }

        @Override
        public void setProperty(long nodeId, double[] doubles, int index, Random random) {
            doubles[index] = random.nextLong(min,max);
        }


    }

    @Test
    void test(){
        int checks=0;
        int nStart = 19;
        int nEnd=20;
        int seedStart=0;
        int seedEnd=100;
        for (int n=nStart;n<nEnd;++n){
            for (int seed=seedStart; seed< seedEnd;++seed) {
                int dEnd = Math.min(n-1,10);
                for (int d = 1; d < dEnd; ++d) {
                    RelationshipDistribution distribution = RelationshipDistribution.UNIFORM;
                    var graph = RandomGraphGenerator.builder()
                        .nodeCount(n)
                        .seed(seed)
                        .averageDegree(d)
                        .aggregation(Aggregation.SUM)
                        .relationshipDistribution(distribution)
                        .relationshipPropertyProducer(new RandomLongsAsDoublesProducer("foo", 1L, 10L))
                        .allowSelfLoops(RandomGraphGeneratorConfig.AllowSelfLoops.NO)
                        .build()
                        .generate();

                    //  printGraph(graph);
                    var maxSolver = MaxFlowSolver.create(graph);

                    for (long s = 0; s < n; ++s) {
                        for (long t = 0; t < n; ++t) {
                            if (s == t) continue;
                            var maxFlowParams = new MaxFlowParameters(
                                new ListInputNodes(List.of(s)),
                                new ListInputNodes(List.of(t)),
                                new Concurrency(1),
                                0.5,
                                false
                            );

                            var maxFlow = new MaxFlow(
                                graph,
                                maxFlowParams,
                                ProgressTracker.NULL_TRACKER,
                                TerminationFlag.RUNNING_TRUE
                            );
                            System.out.println("start: "+distribution+": " + n + " " + "random: " + seed+" deg: "+d + " from: " + s + " to: " + t);
                            // printGraph(graph);
                            var result = maxFlow.compute();
                            if (result.totalFlow() > 0) {
                                checks++;
                                var expected = maxSolver.max_flow((int) s, (int) t);
                                System.out.println(distribution+": " + n + " " + "random: " + seed+" deg: "+d + " from: " + s + " to: " + t + " : " + result.totalFlow() + " " + expected);
                                if (result.totalFlow()!=expected){
                                    System.out.println("after " + checks+" checks failure");
                                    printGraph(graph);
                                }
                                assertThat(result.totalFlow()).isEqualTo(expected);
                            }
                        }
                    }
                }
            }
        }
    }

    void printGraph(Graph graph){
        for (int i=0;i<graph.nodeCount();++i) {
            graph.forEachRelationship(
                i,
                1.0,
                (s, t, w) -> {
                    System.out.println("(a"+s+")-[:R{w:" + w+"]->(a"+t+")");
                    return true;
                }
            );
        }

    }
    static class MaxFlowSolver{
        private final  int n;
        private final  int[][] graph;

        MaxFlowSolver(int n, int[][] graph) {
            this.n = n;
            this.graph = graph;
        }

        static MaxFlowSolver create(Graph graph){
            int n = (int)graph.nodeCount();
            int[][] capacity =new int[n][n];
            for (int i=0;i<n;++i){
                graph.forEachRelationship(i, 1.0, (s,t,w)->{
                    capacity[(int)s][(int)t] = (int)w;
                    return true;
                });
            }


            return  new MaxFlowSolver(n,capacity);
        }
        boolean bfs(int rGraph[][], int s, int t, int parent[]) {
            boolean visited[] = new boolean[n];
            for (int i = 0; i < n; ++i)
                visited[i] = false;

            Queue<Integer> queue = new LinkedList<>();
            queue.add(s);
            visited[s] = true;
            parent[s] = -1;

            while (!queue.isEmpty()) {
                int u = queue.poll();

                for (int v = 0; v < n; v++) {
                    if (!visited[v] && rGraph[u][v] > 0) {
                        queue.add(v);
                        parent[v] = u;
                        visited[v] = true;
                    }
                }
            }

            return visited[t];
        }

        int max_flow(int s, int t) {
            int u, v;
            int rGraph[][] = new int[n][n];

            for (u = 0; u < n; u++)
                for (v = 0; v < n; v++)
                    rGraph[u][v] = graph[u][v];

            int parent[] = new int[n];

            int max_flow = 0;

            while (bfs(rGraph, s, t, parent)) {
                int path_flow = Integer.MAX_VALUE;
                for (v = t; v != s; v = parent[v]) {
                    u = parent[v];
                    path_flow = Math.min(path_flow, rGraph[u][v]);
                }

                for (v = t; v != s; v = parent[v]) {
                    u = parent[v];
                    rGraph[u][v] -= path_flow;
                    rGraph[v][u] += path_flow;
                }

                max_flow += path_flow;
            }

            return max_flow;
        }

    }

    @Test
    void foo(){
        var graph= fromGdl("""
            CREATE
            (a0)-[:R {u: 1.0}]->(a2),
            (a0)-[:R {u: 7.0}]->(a3),
            (a1)-[:R {u: 7.0}]->(a2),
            (a3)-[:R {u: 3.0}]->(a0),
            (a3)-[:R {u: 9.0}]->(a4),
            (a4)-[:R {u: 4.0}]->(a0),
            (a4)-[:R {u: 8.0}]->(a1)
            """, Orientation.NATURAL
        );

        var maxFlowParams = new MaxFlowParameters(
            new ListInputNodes(List.of(graph.toMappedNodeId("a3"))),
            new ListInputNodes(List.of(graph.toOriginalNodeId("a2"))),
            new Concurrency(1),
            0.5D,
            true
        );

        Map<Long,String> map =new HashMap<>();
        for (int i=0;i<5;++i){
            map.put(graph.toMappedNodeId("a"+i),"a"+i);

        }
        var maxFlow = new MaxFlow(
            graph,
            maxFlowParams,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
        );
        FlowResult result = maxFlow.compute();
        System.out.println(result.totalFlow());


        for (var flow : result.flow().toArray()){
            System.out.println(map.get(flow.sourceId())+"-"+flow.flow()+"->" + map.get(flow.targetId()));
        }

    }

    @Test
    void foo2(){
        var graph= fromGdl("""
            CREATE
            (a0),
            (a1),
            (a2),
            (a3),
            (a4),
            (a0)-[:R{w:20.0}]->(a3),
            (a1)-[:R{w:5.0}]->(a0),
            (a1)-[:R{w:1.0}]->(a2),
            (a1)-[:R{w:2.0}]->(a4),
            (a2)-[:R{w:6.0}]->(a1),
            (a2)-[:R{w:7.0}]->(a3),
            (a3)-[:R{w:3.0}]->(a2),
            (a3)-[:R{w:7.0}]->(a4),
            (a4)-[:R{w:17.0}]->(a0),
            (a4)-[:R{w:6.0}]->(a2)
            """, Orientation.NATURAL
        );

        var maxFlowParams = new MaxFlowParameters(
            new ListInputNodes(List.of(graph.toMappedNodeId("a0"))),
            new ListInputNodes(List.of(graph.toOriginalNodeId("a1"))),
            new Concurrency(1),
            0.5,
            true
        );

        Map<Long,String> map =new HashMap<>();
        for (int i=0;i<5;++i){
            map.put(graph.toMappedNodeId("a"+i),"a"+i);

        }
        var maxFlow = new MaxFlow(
            graph,
            maxFlowParams,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
        );
        FlowResult result = maxFlow.compute();
        System.out.println(result.totalFlow());

        for (var flow : result.flow().toArray()){
            System.out.println(map.get(flow.sourceId())+"-"+flow.flow()+"->" + map.get(flow.targetId()));
        }

    }
}
