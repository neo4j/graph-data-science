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
package org.neo4j.gds.beta.filter;

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.TestLog;
import org.neo4j.gds.core.loading.construction.TestMethodRunner;
import org.neo4j.gds.gdl.GdlFactory;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.AdjacencyCursor;
import org.neo4j.gds.api.AdjacencyList;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.Relationships;
import org.neo4j.gds.beta.filter.expression.SemanticErrors;
import org.neo4j.gds.beta.generator.PropertyProducer;
import org.neo4j.gds.beta.generator.RandomGraphGenerator;
import org.neo4j.gds.beta.generator.RelationshipDistribution;
import org.neo4j.gds.config.GraphCreateFromGraphConfig;
import org.neo4j.gds.config.GraphCreateFromStoreConfig;
import org.neo4j.gds.config.ImmutableGraphCreateFromGraphConfig;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.loading.CSRGraphStore;
import org.neo4j.gds.core.loading.CSRGraphStoreUtil;
import org.neo4j.gds.core.loading.IdMapImplementations;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.logging.NullLog;
import org.neo4j.values.storable.NumberType;
import org.opencypher.v9_0.parser.javacc.ParseException;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.TestSupport.assertGraphEquals;
import static org.neo4j.gds.TestSupport.fromGdl;
import static org.neo4j.gds.TestSupport.graphStoreFromGDL;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;

class GraphStoreFilterTest {

    private static GraphCreateFromGraphConfig config(String nodeFilter, String relationshipFilter, int concurrency) {
        return ImmutableGraphCreateFromGraphConfig.builder()
            .concurrency(concurrency)
            .nodeFilter(nodeFilter)
            .relationshipFilter(relationshipFilter)
            .graphName("outputGraph")
            .fromGraphName("inputGraph")
            .originalConfig(GraphCreateFromStoreConfig.emptyWithName("user", "inputGraph"))
            .build();
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.core.loading.construction.TestMethodRunner#idMapImplementation")
    void filterNodesOnLabels(TestMethodRunner runTest) throws Exception {
        runTest.run(() -> {
            var graphStore = graphStoreFromGDL("(:A), (:B), (:C)");

            var filteredGraphStore = filter(graphStore, "n:A", "true");

            assertThat(filteredGraphStore.nodes().availableNodeLabels()).containsExactlyInAnyOrder(NodeLabel.of("A"));
            assertGraphEquals(fromGdl("(:A)"), filteredGraphStore.getUnion());
        });
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.core.loading.construction.TestMethodRunner#idMapImplementation")
    void filterMultipleNodesOnLabels(TestMethodRunner runTest) throws Exception {
        runTest.run(() -> {
            var graphStore = graphStoreFromGDL("(:A), (:B), (:C)");

            var filteredGraphStore = filter(graphStore, "n:A OR n:B", "true");

            assertThat(filteredGraphStore.nodes().availableNodeLabels()).containsExactlyInAnyOrder(
                NodeLabel.of("A"),
                NodeLabel.of("B")
            );
            assertGraphEquals(fromGdl("(:A), (:B)"), filteredGraphStore.getUnion());
        });
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.core.loading.construction.TestMethodRunner#idMapImplementation")
    void filterNodeProperties(TestMethodRunner runTest) throws Exception {
        runTest.run(() -> {
            var graphStore = graphStoreFromGDL(
                "({prop: 42, ignore: 0}), ({prop: 84, ignore: 0}), ({prop: 1337, ignore: 0})");

            var filteredGraphStore = filter(graphStore, "n.prop >= 42 AND n.prop <= 84", "true");

            assertGraphEquals(
                fromGdl("({prop: 42, ignore: 0}), ({prop: 84, ignore: 0})"),
                filteredGraphStore.getUnion()
            );
        });
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.core.loading.construction.TestMethodRunner#idMapImplementation")
    void filterMultipleNodeProperties(TestMethodRunner runTest) throws Exception {
        runTest.run(() -> {
            var graphStore = graphStoreFromGDL(
                "({prop1: 42, prop2: 84}), ({prop1: 42, prop2: 42}), ({prop1: 84, prop2: 84})");

            var filteredGraphStore = filter(graphStore, "n.prop1 = 42 AND n.prop2 = 84", "true");

            assertGraphEquals(fromGdl("({prop1: 42, prop2: 84})"), filteredGraphStore.getUnion());
        });
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.core.loading.construction.TestMethodRunner#idMapImplementation")
    void filterPropertiesAndLabels(TestMethodRunner runTest) throws Exception {
        runTest.run(() -> {
            var graphStore = graphStoreFromGDL("(:A {prop: 42}), (:B {prop: 84}), (:C)");

            var filteredGraphStore = filter(graphStore, "(n:A AND n.prop = 42) OR (n:B AND n.prop = 84)", "true");

            assertGraphEquals(fromGdl("(:A {prop: 42}), (:B {prop: 84})"), filteredGraphStore.getUnion());
        });
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.core.loading.construction.TestMethodRunner#idMapImplementation")
    void filterMissingNodeProperties(TestMethodRunner runTest) throws Exception {
        runTest.run(() -> {
            var graphStore = graphStoreFromGDL("(:A {prop: 42}), (:B)");

            var filteredGraphStore = filter(graphStore, "n.prop = 42", "true");

            assertGraphEquals(fromGdl("(:A {prop: 42})"), filteredGraphStore.getUnion());
        });
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.core.loading.construction.TestMethodRunner#idMapImplementation")
    void keepAllNodeProperties(TestMethodRunner runTest) throws Exception {
        runTest.run(() -> {
            var gdl = "(:A {long: 42L, double: 42.0D, longArray: [42L], floatArray: [42.0F], doubleArray: [42.0D]})";
            var graphStore = graphStoreFromGDL(gdl);

            var filteredGraphStore = filter(graphStore, "true", "true");

            assertThat(filteredGraphStore.schema().nodeSchema()).isEqualTo(graphStore.schema().nodeSchema());
            assertGraphEquals(fromGdl(gdl), filteredGraphStore.getUnion());
        });
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.core.loading.construction.TestMethodRunner#idMapImplementation")
    void removeEmptyNodeSchemaEntries(TestMethodRunner runTest) throws Exception {
        runTest.run(() -> {
            var graphStore = graphStoreFromGDL("(:A {aProp: 42L}), (:B {bProp: 42L})");

            var filteredGraphStore = filter(graphStore, "n:A", "true");

            var aSchema = graphStore
                .schema()
                .nodeSchema()
                .filter(Set.of(NodeLabel.of("A")));

            assertThat(filteredGraphStore.schema().nodeSchema()).isEqualTo(aSchema);
            assertGraphEquals(fromGdl("(:A {aProp: 42L})"), filteredGraphStore.getUnion());
        });
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.core.loading.construction.TestMethodRunner#idMapImplementation")
    void filterRelationshipTypes(TestMethodRunner runTest) throws Exception {
        runTest.run(() -> {
            var graphStore = graphStoreFromGDL("(a)-[:A]->(b), (a)-[:B]->(b), (a)-[:C]->(b)");

            var filteredGraphStore = filter(graphStore, "true", "r:A");

            assertThat(filteredGraphStore.relationshipTypes()).containsExactlyInAnyOrder(RelationshipType.of("A"));
            assertGraphEquals(fromGdl("(a)-[:A]->(b)"), filteredGraphStore.getUnion());
        });
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.core.loading.construction.TestMethodRunner#idMapImplementation")
    void filterMultipleRelationshipTypes(TestMethodRunner runTest) throws Exception {
        runTest.run(() -> {
            var graphStore = graphStoreFromGDL("(a)-[:A]->(b), (a)-[:B]->(b), (a)-[:C]->(b)");

            var filteredGraphStore = filter(graphStore, "true", "r:A OR r:B");

            assertThat(filteredGraphStore.relationshipTypes()).containsExactlyInAnyOrder(
                RelationshipType.of("A"),
                RelationshipType.of("B")
            );
            assertGraphEquals(fromGdl("(a)-[:A]->(b), (a)-[:B]->(b)"), filteredGraphStore.getUnion());
        });
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.core.loading.construction.TestMethodRunner#idMapImplementation")
    void filterRelationshipProperties(TestMethodRunner runTest) throws Exception {
        runTest.run(() -> {
            var graphStore = graphStoreFromGDL(
                "  (a)-[{prop: 42, ignore: 0}]->(b)" +
                ", (a)-[{prop: 84, ignore: 0}]->(b)" +
                ", (a)-[{prop: 1337, ignore: 0}]->(b)"
            );

            var filteredGraphStore = filter(graphStore, "true", "r.prop >= 42 AND r.prop <= 84");

            assertGraphEquals(
                fromGdl("(a)-[{prop: 42, ignore: 0}]->(b), (a)-[{prop: 84, ignore: 0}]->(b)"),
                filteredGraphStore.getUnion()
            );
        });
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.core.loading.construction.TestMethodRunner#idMapImplementation")
    void filterMultipleRelationshipProperties(TestMethodRunner runTest) throws Exception {
        runTest.run(() -> {
            var graphStore = graphStoreFromGDL(
                "  (a)-[{prop1: 42, prop2: 84}]->(b)" +
                ", (a)-[{prop1: 42, prop2: 42}]->(b)" +
                ", (a)-[{prop1: 84, prop2: 84}]->(b)"
            );

            var filteredGraphStore = filter(graphStore, "true", "r.prop1 = 42 AND r.prop2 = 84");

            assertGraphEquals(fromGdl("(a)-[{prop1: 42, prop2: 84}]->(b)"), filteredGraphStore.getUnion());
        });
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.core.loading.construction.TestMethodRunner#idMapImplementation")
    void filterMissingRelationshipProperties(TestMethodRunner runTest) throws Exception {
        runTest.run(() -> {
            var graphStore = graphStoreFromGDL("(a)-[{prop1: 42}]->(b), (a)-[]->(b)");

            var filteredGraphStore = filter(graphStore, "true", "r.prop1 = 42");

            assertGraphEquals(fromGdl("(a)-[{prop1: 42}]->(b)"), filteredGraphStore.getUnion());
        });
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.core.loading.construction.TestMethodRunner#idMapImplementation")
    void keepAllRelationshipProperties(TestMethodRunner runTest) throws Exception {
        runTest.run(() -> {
            var gdl = "()-[:A {double: 42.0D, anotherDouble: 42.0D, yetAnotherDouble: 42.0D}]->()";
            var graphStore = graphStoreFromGDL(gdl);

            var filteredGraphStore = filter(graphStore, "true", "true");

            assertThat(filteredGraphStore.schema().nodeSchema()).isEqualTo(graphStore.schema().nodeSchema());
            assertGraphEquals(fromGdl(gdl), filteredGraphStore.getUnion());
        });
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.core.loading.construction.TestMethodRunner#idMapImplementation")
    void removeEmptyRelationshipSchemaEntries(TestMethodRunner runTest) throws Exception {
        runTest.run(() -> {
            var graphStore = graphStoreFromGDL("(a)-[:A {aProp: 42L}]->(b), (a)-[:B {bProp: 42L}]->(b)");

            var filteredGraphStore = filter(graphStore, "true", "r:A");

            var aSchema = graphStore
                .schema()
                .relationshipSchema()
                .filter(Set.of(RelationshipType.of("A")));

            assertThat(filteredGraphStore.schema().relationshipSchema()).isEqualTo(aSchema);
            assertGraphEquals(fromGdl("(a)-[:A {aProp: 42L}]->(b)"), filteredGraphStore.getUnion());
        });
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.core.loading.construction.TestMethodRunner#idMapImplementation")
    void filterOnStar(TestMethodRunner runTest) throws Exception {
        runTest.run(() -> {
            var graphStore = graphStoreFromGDL("(a)-[:A {aProp: 42L}]->(b), (a)-[:B {bProp: 42L}]->(b)");

            var filteredGraphStore = filter(graphStore, "*", "*");

            assertGraphEquals(graphStore.getUnion(), filteredGraphStore.getUnion());
        });
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.core.loading.construction.TestMethodRunner#idMapImplementation")
    void supportSeparateIdSpaces(TestMethodRunner runTest) throws Exception {
        runTest.run(() -> {
            var nextId = new MutableLong(42_1337L);
            var graphStore = GdlFactory.builder()
                .gdlGraph("(a:A)-[:REL1 {aProp: 42L}]->(b:B), (a)-[:REL2 {bProp: 43L}]->(b)")
                .nodeIdFunction(nextId::getAndIncrement)
                .build()
                .build()
                .graphStore();

            assertThat(graphStore.nodeCount()).isEqualTo(2L);
            assertThat(graphStore.nodes().highestNeoId()).isEqualTo(42_1338L);

            var filteredGraphStore = filter(graphStore, "n:A OR n:B", "r.aProp = 42");
            assertGraphEquals(fromGdl("(a:A)-[:REL1 {aProp: 42L}]->(b:B)"), filteredGraphStore.getUnion());
        });
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.core.loading.construction.TestMethodRunner#idMapImplementation")
    void supportNonOverlappingNodeIdSpace(TestMethodRunner runTest) throws Exception {
        runTest.run(() -> {        // nodes will end up having ids 0, 1, 2, 3 ..
            var graphStore = graphStoreFromGDL("(:A),(:A),(a:B),(a)-->(:B)");
            // .. but only nodes 2 and 3 remain ..
            var filteredGraphStore = filter(graphStore, "n:B", "*");
            // .. and become 0 and 1 in output graph.
            // Assert that the relationship is correctly attached.
            assertGraphEquals(fromGdl("(:B)-->(:B)"), filteredGraphStore.getUnion());
        });
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.core.loading.construction.TestMethodRunner#idMapImplementation")
    void logProgress(TestMethodRunner runTest) throws Exception {
        runTest.run(() -> {
            var relType = RelationshipType.of("REL1");

            var randomGraph = RandomGraphGenerator.builder()
                .nodeCount(100)
                .averageDegree(10)
                .seed(42)
                .nodeLabelProducer(nodeId ->
                    (nodeId % 2 == 0)
                        ? new NodeLabel[]{NodeLabel.of("A")}
                        : new NodeLabel[]{NodeLabel.of("B")}
                )
                .addNodePropertyProducer(NodeLabel.of("A"), PropertyProducer.fixedDouble("foo", 42))
                .addNodePropertyProducer(NodeLabel.of("A"), PropertyProducer.fixedDouble("bar", 1337))
                .addNodePropertyProducer(NodeLabel.of("B"), PropertyProducer.fixedDouble("baz", 42))
                .addNodePropertyProducer(NodeLabel.of("B"), PropertyProducer.fixedDouble("bam", 1337))
                .relationshipType(relType)
                .relationshipDistribution(RelationshipDistribution.POWER_LAW)
                .relationshipPropertyProducer(PropertyProducer.randomDouble("foobar", 0, 1))
                .build()
                .generate();

            var graphStore = CSRGraphStoreUtil.createFromGraph(
                TestDatabaseIdRepository.randomNamedDatabaseId(),
                randomGraph,
                relType.name(),
                Optional.of("foobar"),
                1,
                AllocationTracker.empty()
            );

            graphStore.addRelationshipType(
                RelationshipType.of("REL2"),
                Optional.of("foobaz"),
                Optional.of(NumberType.INTEGRAL),
                randomGraph.relationships()
            );

            var log = new TestLog();

            GraphStoreFilter.filter(
                graphStore,
                config("*", "*", 1),
                Pools.DEFAULT,
                log,
                AllocationTracker.empty()
            );

            if (IdMapImplementations.useBitIdMap()) {
                assertThat(log.getMessages(TestLog.INFO))
                    // avoid asserting on the thread id
                    .extracting(removingThreadId())
                    .contains(
                        "GraphStore Filter :: Prepare node ids :: Start",
                        "GraphStore Filter :: Prepare node ids :: Create id array :: Start",
                        "GraphStore Filter :: Prepare node ids :: Create id array :: Finished",
                        "GraphStore Filter :: Prepare node ids :: Sort id array :: Start",
                        "GraphStore Filter :: Prepare node ids :: Sort id array :: Finished",
                        "GraphStore Filter :: Prepare node ids :: Finished"
                    );
            }

            assertThat(log.getMessages(TestLog.INFO))
                // avoid asserting on the thread id
                .extracting(removingThreadId())
                .contains(
                    "GraphStore Filter :: Start",
                    "GraphStore Filter :: Nodes :: Start",
                    "GraphStore Filter :: Nodes 100%",
                    "GraphStore Filter :: Nodes :: Finished",
                    "GraphStore Filter :: Node properties :: Start",
                    "GraphStore Filter :: Node properties :: Label 1 of 2 :: Start",
                    "GraphStore Filter :: Node properties :: Label 1 of 2 :: Property 1 of 2 :: Start",
                    "GraphStore Filter :: Node properties :: Label 1 of 2 :: Property 1 of 2 99%",
                    "GraphStore Filter :: Node properties :: Label 1 of 2 :: Property 1 of 2 :: Finished",
                    "GraphStore Filter :: Node properties :: Label 1 of 2 :: Property 2 of 2 :: Start",
                    "GraphStore Filter :: Node properties :: Label 1 of 2 :: Property 2 of 2 99%",
                    "GraphStore Filter :: Node properties :: Label 1 of 2 :: Property 2 of 2 :: Finished",
                    "GraphStore Filter :: Node properties :: Label 1 of 2 :: Finished",
                    "GraphStore Filter :: Node properties :: Label 2 of 2 :: Start",
                    "GraphStore Filter :: Node properties :: Label 2 of 2 :: Property 1 of 2 :: Start",
                    "GraphStore Filter :: Node properties :: Label 2 of 2 :: Property 1 of 2 99%",
                    "GraphStore Filter :: Node properties :: Label 2 of 2 :: Property 1 of 2 :: Finished",
                    "GraphStore Filter :: Node properties :: Label 2 of 2 :: Property 2 of 2 :: Start",
                    "GraphStore Filter :: Node properties :: Label 2 of 2 :: Property 2 of 2 99%",
                    "GraphStore Filter :: Node properties :: Label 2 of 2 :: Property 2 of 2 :: Finished",
                    "GraphStore Filter :: Node properties :: Label 2 of 2 :: Finished",
                    "GraphStore Filter :: Node properties :: Finished",
                    "GraphStore Filter :: Relationship types 1 of 2 :: Start",
                    "GraphStore Filter :: Relationship types 1 of 2 99%",
                    "GraphStore Filter :: Relationship types 1 of 2 :: Finished",
                    "GraphStore Filter :: Relationship types 2 of 2 :: Start",
                    "GraphStore Filter :: Relationship types 2 of 2 99%",
                    "GraphStore Filter :: Relationship types 2 of 2 :: Finished",
                    "GraphStore Filter :: Finished"
                );
        });
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.core.loading.construction.TestMethodRunner#idMapImplementation")
    void testMultiThreadedFiltering(TestMethodRunner runTest) throws Exception {
        runTest.run(() -> {
            var labelA = new NodeLabel[]{NodeLabel.of("A")};
            var labelB = new NodeLabel[]{NodeLabel.of("B")};

            var concurrency = 4;

            var generatedGraph = RandomGraphGenerator
                .builder()
                .nodeCount(100_000)
                .nodeLabelProducer((node) -> ThreadLocalRandom.current().nextDouble(0, 1) > 0.5 ? labelA : labelB)
                .nodePropertyProducer(PropertyProducer.randomDouble("nodeProp", 0, 1))
                .relationshipDistribution(RelationshipDistribution.POWER_LAW)
                .relationshipPropertyProducer(PropertyProducer.randomDouble("relProp", 0, 1))
                .averageDegree(5)
                .seed(42)
                .build()
                .generate();

            var graphStore = CSRGraphStoreUtil.createFromGraph(
                TestDatabaseIdRepository.randomNamedDatabaseId(),
                generatedGraph,
                "REL",
                Optional.empty(),
                concurrency,
                AllocationTracker.empty()
            );

            var graph = graphStore.getUnion();

            assertGraphEquals(graph, filter(graphStore, "*", "*", concurrency).getUnion());
        });
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.core.loading.construction.TestMethodRunner#idMapImplementation")
    void testNonOverlappingNodeIdSpace(TestMethodRunner runTest) throws Exception {
        runTest.run(() -> {
            var nodeCount = 10_000;
            var maxOriginalId = 100_000;
            var concurrency = 4;
            var tracker = AllocationTracker.empty();

            // Create an id map where the original id space
            // does not overlap with the internal id space.
            var builder = GraphFactory.initNodesBuilder()
                .nodeCount(nodeCount)
                .maxOriginalId(maxOriginalId)
                .tracker(tracker)
                .build();

            LongStream.range(maxOriginalId - nodeCount, maxOriginalId).forEach(builder::addNode);

            var graphStore = CSRGraphStore.of(
                DatabaseIdFactory.from("neo4j", UUID.randomUUID()),
                builder.build().nodeMapping(),
                Map.of(),
                Map.of(
                    RelationshipType.ALL_RELATIONSHIPS,
                    Relationships.of(0, Orientation.NATURAL, false, new EmptyAdjacencyList()).topology()
                ),
                Map.of(),
                concurrency,
                tracker
            );

            var filteredGraphStore = filter(graphStore, "*", "*", concurrency);

            assertThat(graphStore.nodeCount()).isEqualTo(nodeCount);
            assertThat(filteredGraphStore.nodeCount()).isEqualTo(nodeCount);
        });
    }

    private GraphStore filter(GraphStore graphStore, String nodeFilter, String relationshipFilter) throws
        SemanticErrors,
        ParseException {
        return filter(graphStore, nodeFilter, relationshipFilter, 1);
    }

    private GraphStore filter(
        GraphStore graphStore,
        String nodeFilter,
        String relationshipFilter,
        int concurrency
    ) throws
        ParseException,
        SemanticErrors {
        return GraphStoreFilter.filter(
            graphStore,
            config(nodeFilter, relationshipFilter, concurrency),
            Pools.DEFAULT,
            NullLog.getInstance(),
            AllocationTracker.empty()
        );
    }

    private static class EmptyAdjacencyList implements AdjacencyList {

        @Override
        public int degree(long node) {
            return 0;
        }

        @Override
        public AdjacencyCursor adjacencyCursor(long node, double fallbackValue) {
            return AdjacencyCursor.EmptyAdjacencyCursor.INSTANCE;
        }

        @Override
        public AdjacencyCursor rawAdjacencyCursor() {
            return AdjacencyCursor.EmptyAdjacencyCursor.INSTANCE;
        }

        @Override
        public void close() {

        }
    }
}
