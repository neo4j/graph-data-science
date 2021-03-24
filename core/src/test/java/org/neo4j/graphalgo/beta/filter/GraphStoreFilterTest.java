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
package org.neo4j.graphalgo.beta.filter;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.TestLog;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.beta.filter.expression.SemanticErrors;
import org.neo4j.graphalgo.beta.generator.PropertyProducer;
import org.neo4j.graphalgo.beta.generator.RandomGraphGenerator;
import org.neo4j.graphalgo.beta.generator.RelationshipDistribution;
import org.neo4j.graphalgo.config.GraphCreateFromGraphConfig;
import org.neo4j.graphalgo.config.GraphCreateFromStoreConfig;
import org.neo4j.graphalgo.config.ImmutableGraphCreateFromGraphConfig;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.loading.CSRGraphStoreUtil;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.logging.NullLog;
import org.neo4j.values.storable.NumberType;
import org.opencypher.v9_0.parser.javacc.ParseException;

import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;
import static org.neo4j.graphalgo.TestSupport.fromGdl;
import static org.neo4j.graphalgo.TestSupport.graphStoreFromGDL;
import static org.neo4j.graphalgo.assertj.Extractors.removingThreadId;

class GraphStoreFilterTest {

    private static GraphCreateFromGraphConfig config(String nodeFilter, String relationshipFilter) {
        return ImmutableGraphCreateFromGraphConfig.builder()
            .concurrency(1)
            .nodeFilter(nodeFilter)
            .relationshipFilter(relationshipFilter)
            .graphName("outputGraph")
            .fromGraphName("inputGraph")
            .originalConfig(GraphCreateFromStoreConfig.emptyWithName("user", "inputGraph"))
            .build();
    }

    @Test
    void filterNodesOnLabels() throws ParseException, SemanticErrors {
        var graphStore = graphStoreFromGDL("(:A), (:B), (:C)");

        var filteredGraphStore = filter(graphStore, "n:A", "true");

        assertThat(filteredGraphStore.nodes().availableNodeLabels()).containsExactlyInAnyOrder(NodeLabel.of("A"));
        assertGraphEquals(fromGdl("(:A)"), filteredGraphStore.getUnion());
    }

    @Test
    void filterMultipleNodesOnLabels() throws ParseException, SemanticErrors {
        var graphStore = graphStoreFromGDL("(:A), (:B), (:C)");

        var filteredGraphStore = filter(graphStore, "n:A OR n:B", "true");

        assertThat(filteredGraphStore.nodes().availableNodeLabels()).containsExactlyInAnyOrder(
            NodeLabel.of("A"),
            NodeLabel.of("B")
        );
        assertGraphEquals(fromGdl("(:A), (:B)"), filteredGraphStore.getUnion());
    }

    @Test
    void filterNodeProperties() throws ParseException, SemanticErrors {
        var graphStore = graphStoreFromGDL(
            "({prop: 42, ignore: 0}), ({prop: 84, ignore: 0}), ({prop: 1337, ignore: 0})");

        var filteredGraphStore = filter(graphStore, "n.prop >= 42 AND n.prop <= 84", "true");

        assertGraphEquals(fromGdl("({prop: 42, ignore: 0}), ({prop: 84, ignore: 0})"), filteredGraphStore.getUnion());
    }

    @Test
    void filterMultipleNodeProperties() throws ParseException, SemanticErrors {
        var graphStore = graphStoreFromGDL(
            "({prop1: 42, prop2: 84}), ({prop1: 42, prop2: 42}), ({prop1: 84, prop2: 84})");

        var filteredGraphStore = filter(graphStore, "n.prop1 = 42 AND n.prop2 = 84", "true");

        assertGraphEquals(fromGdl("({prop1: 42, prop2: 84})"), filteredGraphStore.getUnion());
    }

    @Test
    void filterPropertiesAndLabels() throws ParseException, SemanticErrors {
        var graphStore = graphStoreFromGDL("(:A {prop: 42}), (:B {prop: 84}), (:C)");

        var filteredGraphStore = filter(graphStore, "(n:A AND n.prop = 42) OR (n:B AND n.prop = 84)", "true");

        assertGraphEquals(fromGdl("(:A {prop: 42}), (:B {prop: 84})"), filteredGraphStore.getUnion());
    }

    @Test
    void filterMissingNodeProperties() throws ParseException, SemanticErrors {
        var graphStore = graphStoreFromGDL("(:A {prop: 42}), (:B)");

        var filteredGraphStore = filter(graphStore, "n.prop = 42", "true");

        assertGraphEquals(fromGdl("(:A {prop: 42})"), filteredGraphStore.getUnion());
    }

    @Test
    void keepAllNodeProperties() throws ParseException, SemanticErrors {
        var gdl = "(:A {long: 42L, double: 42.0D, longArray: [42L], floatArray: [42.0F], doubleArray: [42.0D]})";
        var graphStore = graphStoreFromGDL(gdl);

        var filteredGraphStore = filter(graphStore, "true", "true");

        assertThat(filteredGraphStore.schema().nodeSchema()).isEqualTo(graphStore.schema().nodeSchema());
        assertGraphEquals(fromGdl(gdl), filteredGraphStore.getUnion());
    }

    @Test
    void removeEmptyNodeSchemaEntries() throws ParseException, SemanticErrors {
        var graphStore = graphStoreFromGDL("(:A {aProp: 42L}), (:B {bProp: 42L})");

        var filteredGraphStore = filter(graphStore, "n:A", "true");

        var aSchema = graphStore
            .schema()
            .nodeSchema()
            .filter(Set.of(NodeLabel.of("A")));

        assertThat(filteredGraphStore.schema().nodeSchema()).isEqualTo(aSchema);
        assertGraphEquals(fromGdl("(:A {aProp: 42L})"), filteredGraphStore.getUnion());
    }

    @Test
    void filterRelationshipTypes() throws ParseException, SemanticErrors {
        var graphStore = graphStoreFromGDL("(a)-[:A]->(b), (a)-[:B]->(b), (a)-[:C]->(b)");

        var filteredGraphStore = filter(graphStore, "true", "r:A");

        assertThat(filteredGraphStore.relationshipTypes()).containsExactlyInAnyOrder(RelationshipType.of("A"));
        assertGraphEquals(fromGdl("(a)-[:A]->(b)"), filteredGraphStore.getUnion());
    }

    @Test
    void filterMultipleRelationshipTypes() throws ParseException, SemanticErrors {
        var graphStore = graphStoreFromGDL("(a)-[:A]->(b), (a)-[:B]->(b), (a)-[:C]->(b)");

        var filteredGraphStore = filter(graphStore, "true", "r:A OR r:B");

        assertThat(filteredGraphStore.relationshipTypes()).containsExactlyInAnyOrder(
            RelationshipType.of("A"),
            RelationshipType.of("B")
        );
        assertGraphEquals(fromGdl("(a)-[:A]->(b), (a)-[:B]->(b)"), filteredGraphStore.getUnion());
    }

    @Test
    void filterRelationshipProperties() throws ParseException, SemanticErrors {
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
    }

    @Test
    void filterMultipleRelationshipProperties() throws ParseException, SemanticErrors {
        var graphStore = graphStoreFromGDL(
            "  (a)-[{prop1: 42, prop2: 84}]->(b)" +
            ", (a)-[{prop1: 42, prop2: 42}]->(b)" +
            ", (a)-[{prop1: 84, prop2: 84}]->(b)"
        );

        var filteredGraphStore = filter(graphStore, "true", "r.prop1 = 42 AND r.prop2 = 84");

        assertGraphEquals(fromGdl("(a)-[{prop1: 42, prop2: 84}]->(b)"), filteredGraphStore.getUnion());
    }

    @Test
    void filterMissingRelationshipProperties() throws ParseException, SemanticErrors {
        var graphStore = graphStoreFromGDL("(a)-[{prop1: 42}]->(b), (a)-[]->(b)");

        var filteredGraphStore = filter(graphStore, "true", "r.prop1 = 42");

        assertGraphEquals(fromGdl("(a)-[{prop1: 42}]->(b)"), filteredGraphStore.getUnion());
    }

    @Test
    void keepAllRelationshipProperties() throws ParseException, SemanticErrors {
        var gdl = "()-[:A {double: 42.0D, anotherDouble: 42.0D, yetAnotherDouble: 42.0D}]->()";
        var graphStore = graphStoreFromGDL(gdl);

        var filteredGraphStore = filter(graphStore, "true", "true");

        assertThat(filteredGraphStore.schema().nodeSchema()).isEqualTo(graphStore.schema().nodeSchema());
        assertGraphEquals(fromGdl(gdl), filteredGraphStore.getUnion());
    }

    @Test
    void removeEmptyRelationshipSchemaEntries() throws ParseException, SemanticErrors {
        var graphStore = graphStoreFromGDL("(a)-[:A {aProp: 42L}]->(b), (a)-[:B {bProp: 42L}]->(b)");

        var filteredGraphStore = filter(graphStore, "true", "r:A");

        var aSchema = graphStore
            .schema()
            .relationshipSchema()
            .filter(Set.of(RelationshipType.of("A")));

        assertThat(filteredGraphStore.schema().relationshipSchema()).isEqualTo(aSchema);
        assertGraphEquals(fromGdl("(a)-[:A {aProp: 42L}]->(b)"), filteredGraphStore.getUnion());
    }

    @Test
    void filterOnStar() throws ParseException, SemanticErrors {
        var graphStore = graphStoreFromGDL("(a)-[:A {aProp: 42L}]->(b), (a)-[:B {bProp: 42L}]->(b)");

        var filteredGraphStore = filter(graphStore, "*", "*");

        assertGraphEquals(graphStore.getUnion(), filteredGraphStore.getUnion());
    }

    @Test
    void logProgress() throws ParseException, SemanticErrors {
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
            .addNodePropertyProducer(NodeLabel.of("A"), PropertyProducer.fixed("foo", 42))
            .addNodePropertyProducer(NodeLabel.of("A"), PropertyProducer.fixed("bar", 1337))
            .addNodePropertyProducer(NodeLabel.of("B"), PropertyProducer.fixed("baz", 42))
            .addNodePropertyProducer(NodeLabel.of("B"), PropertyProducer.fixed("bam", 1337))
            .relationshipType(relType)
            .relationshipDistribution(RelationshipDistribution.POWER_LAW)
            .relationshipPropertyProducer(PropertyProducer.random("foobar", 0, 1))
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
            config("*", "*"),
            Pools.DEFAULT,
            log,
            AllocationTracker.empty()
        );

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
    }

    private GraphStore filter(GraphStore graphStore, String nodeFilter, String relationshipFilter) throws
        ParseException,
        SemanticErrors {
        return GraphStoreFilter.filter(
            graphStore,
            config(nodeFilter, relationshipFilter),
            Pools.DEFAULT,
            NullLog.getInstance(),
            AllocationTracker.empty()
        );
    }
}
