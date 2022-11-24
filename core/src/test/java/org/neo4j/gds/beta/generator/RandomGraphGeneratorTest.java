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
package org.neo4j.gds.beta.generator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.api.schema.NodeSchema;
import org.neo4j.gds.api.schema.RelationshipSchema;
import org.neo4j.gds.config.RandomGraphGeneratorConfig;
import org.neo4j.gds.config.RandomGraphGeneratorConfig.AllowSelfLoops;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.huge.HugeGraph;
import org.neo4j.gds.core.loading.construction.NodeLabelTokens;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.Orientation.NATURAL;
import static org.neo4j.gds.Orientation.UNDIRECTED;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class RandomGraphGeneratorTest {

    @Test
    void shouldGenerateRelsUniformDistributed() {
        int nbrNodes = 10;
        long avgDeg = 5L;

        RandomGraphGenerator randomGraphGenerator = RandomGraphGenerator
            .builder()
            .nodeCount(nbrNodes)
            .averageDegree(avgDeg)
            .relationshipDistribution(RelationshipDistribution.UNIFORM)
            .build();
        HugeGraph graph = randomGraphGenerator.generate();

        assertEquals(graph.nodeCount(), nbrNodes);
        assertEquals(nbrNodes * avgDeg, graph.relationshipCount());

        graph.forEachNode((nodeId) -> {
            assertEquals(avgDeg, graph.degree(nodeId));
            return true;
        });
    }

    @Test
    void shouldGenerateRelsPowerLawDistributed() {
        int nbrNodes = 10_000;
        long avgDeg = 5L;

        RandomGraphGenerator randomGraphGenerator = RandomGraphGenerator
            .builder()
            .nodeCount(nbrNodes)
            .averageDegree(avgDeg)
            .relationshipDistribution(RelationshipDistribution.POWER_LAW)
            .build();
        HugeGraph graph = randomGraphGenerator.generate();

        assertEquals(graph.nodeCount(), nbrNodes);
        assertEquals((double) nbrNodes * avgDeg, graph.relationshipCount(), 1_000D);

        var previousAvgDegree = Double.POSITIVE_INFINITY;
        var bucketSize = nbrNodes / 10;
        for (var start = 0L; start < nbrNodes; start += bucketSize) {
            var end = Math.min(start + bucketSize, nbrNodes);
            var avgDegreeInBucket = LongStream.range(start, end).mapToInt(graph::degree).average().orElse(0.0);
            assertThat(avgDegreeInBucket).isLessThanOrEqualTo(previousAvgDegree);
            previousAvgDegree = avgDegreeInBucket;
        }
    }

    @Test
    void shouldNotGenerateSelfLoops() {
        int nbrNodes = 1000;
        long avgDeg = 5L;

        RandomGraphGenerator randomGraphGenerator = RandomGraphGenerator
            .builder()
            .nodeCount(nbrNodes)
            .averageDegree(avgDeg)
            .relationshipDistribution(RelationshipDistribution.POWER_LAW)
            .orientation(Orientation.UNDIRECTED)
            .allowSelfLoops(AllowSelfLoops.NO)
            .build();
        HugeGraph graph = randomGraphGenerator.generate();

        for (long nodeId = 0; nodeId < graph.nodeCount(); nodeId++) {
            graph.forEachRelationship(nodeId, (src, trg) -> {
                Assertions.assertNotEquals(src, trg);
                return true;
            });
        }
    }

    @Test
    void shouldGenerateRelsRandomDistributed() {
        int nbrNodes = 1_000;
        long avgDeg = 5L;

        RandomGraphGenerator randomGraphGenerator = RandomGraphGenerator
            .builder()
            .nodeCount(nbrNodes)
            .averageDegree(avgDeg)
            .relationshipDistribution(RelationshipDistribution.RANDOM)
            .build();
        HugeGraph graph = randomGraphGenerator.generate();

        assertEquals(graph.nodeCount(), nbrNodes);

        AtomicLong actualRelCount = new AtomicLong();
        graph.forEachNode(node -> {
            actualRelCount.addAndGet(graph.degree(node));
            return true;
        });

        double actualAverage = actualRelCount.get() / (double) nbrNodes;

        assertEquals((double) avgDeg, actualAverage, 0.5D);
    }

    @Test
    void shouldGenerateRelationshipPropertiesWithFixedValue() {
        int nbrNodes = 10;
        long avgDeg = 5L;

        double fixedValue = 42D;
        RandomGraphGenerator randomGraphGenerator = RandomGraphGenerator
            .builder()
            .nodeCount(nbrNodes)
            .averageDegree(avgDeg)
            .relationshipDistribution(RelationshipDistribution.UNIFORM)
            .relationshipPropertyProducer(PropertyProducer.fixedDouble("property", fixedValue))
            .build();
        HugeGraph graph = randomGraphGenerator.generate();

        graph.forEachNode((nodeId) -> {
            graph.forEachRelationship(nodeId, Double.NaN, (s, t, p) -> {
                assertEquals(fixedValue, p);
                return true;
            });
            return true;
        });
    }

    @Test
    void shouldGenerateRelationshipWithRandom() {
        int lowerBound = -10;
        int upperBound = 10;

        RandomGraphGenerator randomGraphGenerator = RandomGraphGenerator
            .builder()
            .nodeCount(10)
            .averageDegree(5L)
            .relationshipDistribution(RelationshipDistribution.UNIFORM)
            .relationshipPropertyProducer(PropertyProducer.randomDouble("prop", lowerBound, upperBound))
            .build();
        HugeGraph graph = randomGraphGenerator.generate();

        graph.forEachNode((nodeId) -> {
            graph.forEachRelationship(nodeId, Double.NaN, (s, t, p) -> {
                assertTrue(p >= lowerBound);
                assertTrue(p <= upperBound);
                return true;
            });
            return true;
        });
    }

    @Test
    void shouldGenerateNodeLabels() {
        var aLabel = NodeLabelTokens.of("A");
        var bLabel = NodeLabelTokens.of("B");

        HugeGraph graph = RandomGraphGenerator
            .builder()
            .nodeCount(10)
            .averageDegree(2)
            .relationshipDistribution(RelationshipDistribution.UNIFORM)
            .nodeLabelProducer(nodeId -> nodeId % 2 == 0 ? aLabel : bLabel)
            .build()
            .generate();

        graph.forEachNode(nodeId -> {
            var nodeLabels = graph.nodeLabels(nodeId);
            assertEquals(1, nodeLabels.size());
            if (nodeId % 2 == 0) {
                assertTrue(nodeLabels.contains(NodeLabel.of("A")),
                    formatWithLocale("node %d should have label A", nodeId)
                );
            } else {
                assertTrue(nodeLabels.contains(NodeLabel.of("B")),
                    formatWithLocale("node %d should have label B", nodeId)
                );
            }
            return true;
        });
    }

    @Test
    void shouldGenerateNodeProperties() {
        int lowerBound = 0;
        int upperBound = 1;
        HugeGraph graph = RandomGraphGenerator
            .builder()
            .nodeCount(10)
            .averageDegree(2)
            .relationshipDistribution(RelationshipDistribution.UNIFORM)
            .nodePropertyProducer(PropertyProducer.randomDouble("foo", lowerBound, upperBound))
            .build()
            .generate();

        NodePropertyValues nodePropertyValues = graph.nodeProperties("foo");
        graph.forEachNode(nodeId -> {
            double value = nodePropertyValues.doubleValue(nodeId);
            assertTrue(lowerBound <= value && value <= upperBound);
            return true;
        });
    }

    @Test
    void shouldGenerateEmbeddingsForNodeProperties() {
        float lowerBound = 0f;
        float upperBound = 1f;
        HugeGraph graph = RandomGraphGenerator
            .builder()
            .nodeCount(10)
            .averageDegree(2)
            .relationshipDistribution(RelationshipDistribution.UNIFORM)
            .nodePropertyProducer(PropertyProducer.randomEmbedding("foo", 42, lowerBound, upperBound))
            .build()
            .generate();

        NodePropertyValues nodePropertyValues = graph.nodeProperties("foo");
        graph.forEachNode(nodeId -> {
            float[] values = nodePropertyValues.floatArrayValue(nodeId);
            assertEquals(values.length, 42);
            for (float value : values) {
                assertThat(value).isBetween(lowerBound, upperBound);
            }
            return true;
        });
    }

    @Test
    void shouldGenerateNodeLabelsAndProperties() {
        var aLabel = NodeLabelTokens.ofNodeLabels(NodeLabel.of("A"), NodeLabel.ALL_NODES);
        var bLabel = NodeLabelTokens.ofNodeLabels(NodeLabel.of("B"), NodeLabel.ALL_NODES);

        HugeGraph graph = RandomGraphGenerator
            .builder()
            .nodeCount(10)
            .averageDegree(2)
            .relationshipDistribution(RelationshipDistribution.UNIFORM)
            .nodeLabelProducer(nodeId -> nodeId % 2 == 0 ? aLabel : bLabel)
            .nodePropertyProducer(PropertyProducer.fixedDouble("all", 1337.0))
            .addNodePropertyProducer(NodeLabel.of("A"), PropertyProducer.fixedDouble("foo", 42.0))
            .addNodePropertyProducer(NodeLabel.of("B"), PropertyProducer.fixedDouble("bar", 84.0))
            .addNodePropertyProducer(NodeLabel.of("B"), PropertyProducer.fixedDouble("baz", 23.0))
            .build()
            .generate();

        var allProperties = graph.nodeProperties("all");
        var fooProperties = graph.nodeProperties("foo");
        var barProperties = graph.nodeProperties("bar");
        var bazProperties = graph.nodeProperties("baz");

        graph.forEachNode(nodeId -> {
            var nodeLabels = graph.nodeLabels(nodeId);
            assertEquals(2, nodeLabels.size());
            assertEquals(1337.0, allProperties.doubleValue(nodeId));
            if (nodeId % 2 == 0) {
                assertTrue(nodeLabels.contains(NodeLabel.of("A")),
                    formatWithLocale("node %d should have label A", nodeId)
                );
                assertEquals(42.0, fooProperties.doubleValue(nodeId));
                assertTrue(Double.isNaN(barProperties.doubleValue(nodeId)));
                assertTrue(Double.isNaN(bazProperties.doubleValue(nodeId)));
            } else {
                assertTrue(nodeLabels.contains(NodeLabel.of("B")),
                    formatWithLocale("node %d should have label B", nodeId)
                );
                assertEquals(84.0, barProperties.doubleValue(nodeId));
                assertEquals(23.0, bazProperties.doubleValue(nodeId));
                assertTrue(Double.isNaN(fooProperties.doubleValue(nodeId)));
            }
            return true;
        });
    }

    @Test
    void shouldNotAddEmptyNodePropertyProducer() {
        NodeLabel aLabel = NodeLabel.of("A");

        HugeGraph graph = RandomGraphGenerator
            .builder()
            .nodeCount(10)
            .averageDegree(2)
            .relationshipDistribution(RelationshipDistribution.UNIFORM)
            .nodePropertyProducer(new PropertyProducer.EmptyPropertyProducer())
            .addNodePropertyProducer(aLabel, new PropertyProducer.EmptyPropertyProducer())
            .build()
            .generate();

        assertThat(graph.nodeProperties()).isEmpty();
    }

    @Test
    void shouldFailForMultiplePropertyNames() {
        assertThatThrownBy(() -> RandomGraphGenerator
            .builder()
            .nodeCount(10)
            .averageDegree(2)
            .relationshipDistribution(RelationshipDistribution.UNIFORM)
            .nodePropertyProducer(PropertyProducer.fixedDouble("name", 1337.0))
            .nodePropertyProducer(PropertyProducer.fixedDouble("name", 42.0))
            .build()
            .generate())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Duplicate node properties with name [name]. " + "The first property producer is [FixedDoubleProducer{propertyName='name', value=1337.0}], " + "the second one is [FixedDoubleProducer{propertyName='name', value=42.0}].");
    }

    @Test
    void shouldFailForMultiplePropertyNamesForLabeledProducers() {
        var aLabel = NodeLabel.of("A");
        var bLabel = NodeLabel.of("B");
        assertThatThrownBy(() -> RandomGraphGenerator
            .builder()
            .nodeCount(10)
            .averageDegree(2)
            .relationshipDistribution(RelationshipDistribution.UNIFORM)
            .nodeLabelProducer((i) -> i % 2 == 0 ? NodeLabelTokens.of(aLabel) : NodeLabelTokens.of(bLabel))
            .addNodePropertyProducer(aLabel, PropertyProducer.fixedDouble("name", 1337.0))
            .addNodePropertyProducer(bLabel, PropertyProducer.fixedDouble("name", 42.0))
            .build()
            .generate())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Duplicate node properties with name [name]. " + "The first property producer is [FixedDoubleProducer{propertyName='name', value=1337.0}], " + "the second one is [FixedDoubleProducer{propertyName='name', value=42.0}].");
    }

    @ParameterizedTest
    @MethodSource("producers")
    void shouldNotFailForMultiplePropertyNamesIfTheProducerIsTheSame(PropertyProducer<?> producer) {
        assertThatCode(() -> RandomGraphGenerator
            .builder()
            .nodeCount(10)
            .averageDegree(2)
            .relationshipDistribution(RelationshipDistribution.UNIFORM)
            .nodePropertyProducer(producer)
            .nodePropertyProducer(producer)
            .build()
            .generate()).doesNotThrowAnyException();
    }

    @ParameterizedTest
    @MethodSource("producers")
    void shouldNotFailForMultiplePropertyNamesIfTheProducerIsTheSameForLabeledProducers(PropertyProducer<?> producer) {
        var aLabel = NodeLabel.of("A");
        var bLabel = NodeLabel.of("B");
        assertThatCode(() -> RandomGraphGenerator
            .builder()
            .nodeCount(10)
            .averageDegree(2)
            .nodeLabelProducer((i) -> i % 2 == 0 ? NodeLabelTokens.of(aLabel) : NodeLabelTokens.of(bLabel))
            .relationshipDistribution(RelationshipDistribution.UNIFORM)
            .addNodePropertyProducer(aLabel, producer)
            .addNodePropertyProducer(bLabel, producer)
            .build()
            .generate()).doesNotThrowAnyException();
    }

    static Stream<PropertyProducer<?>> producers() {
        return Stream.of(PropertyProducer.fixedDouble("name", 42.0),
            PropertyProducer.randomDouble("name", 42.0, 1337.0),
            PropertyProducer.randomEmbedding("name", 21, 42.0f, 1337.0f)
        );
    }

    @Test
    void shouldBeSeedAble() {
        int nbrNodes = 10;
        long avgDeg = 5L;
        long seed = 1337L;

        RandomGraphGenerator randomGraphGenerator = RandomGraphGenerator
            .builder()
            .nodeCount(nbrNodes)
            .averageDegree(avgDeg)
            .relationshipDistribution(RelationshipDistribution.UNIFORM)
            .seed(seed)
            .build();

        RandomGraphGenerator otherRandomGenerator = RandomGraphGenerator
            .builder()
            .nodeCount(nbrNodes)
            .averageDegree(avgDeg)
            .relationshipDistribution(RelationshipDistribution.UNIFORM)
            .seed(seed)
            .build();

        HugeGraph graph1 = randomGraphGenerator.generate();
        HugeGraph graph2 = otherRandomGenerator.generate();

        TestSupport.assertGraphEquals(graph1, graph2);
    }

    @Test
    void shouldProduceCorrectRelationshipCountWithAggregation() {
        var graph = RandomGraphGenerator
            .builder()
            .nodeCount(8)
            .averageDegree(4)
            .seed(42L)
            .aggregation(Aggregation.SINGLE)
            .allowSelfLoops(RandomGraphGeneratorConfig.AllowSelfLoops.NO)
            .relationshipDistribution(RelationshipDistribution.POWER_LAW)
            .orientation(Orientation.UNDIRECTED)
            .build()
            .generate();

        AtomicLong actualRelCount = new AtomicLong();

        graph.forEachNode(node -> {
            actualRelCount.addAndGet(graph.degree(node));
            return true;
        });

        assertEquals(actualRelCount.get(), graph.relationshipCount());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("expectedSchemas")
    void shouldProduceCorrectSchema(
        String name,
        RandomGraphGenerator generator,
        NodeSchema expectedNodeSchema,
        RelationshipSchema expectedRelationshipSchema
    ) {
        var graph = generator.generate();

        assertThat(graph.schema().nodeSchema()).isEqualTo(expectedNodeSchema);
        assertThat(graph.schema().relationshipSchema()).isEqualTo(expectedRelationshipSchema);
    }

    @Test
    void shouldGenerateWithCorrectIdMap() {
        var graph = RandomGraphGenerator
            .builder()
            .nodeCount(100)
            .averageDegree(1)
            .relationshipDistribution(RelationshipDistribution.UNIFORM)
            .seed(123L)
            .aggregation(Aggregation.SINGLE)
            .orientation(Orientation.UNDIRECTED)
            .allowSelfLoops(RandomGraphGeneratorConfig.AllowSelfLoops.NO)
            .build()
            .generate();

        assertThat(graph.availableNodeLabels()).containsExactly(NodeLabel.ALL_NODES);
        assertThat(graph.nodeCount()).isEqualTo(100);
    }

    @Test
    void shouldGenerateWithCorrectIdMapWithLabelProducer() {
        var graph = RandomGraphGenerator
            .builder()
            .nodeCount(100)
            .nodeLabelProducer((nodeId) -> nodeId % 2 == 0 ? NodeLabelTokens.of("LABEL1") : NodeLabelTokens.of("LABEL2"))
            .averageDegree(1)
            .relationshipDistribution(RelationshipDistribution.UNIFORM)
            .seed(123L)
            .aggregation(Aggregation.SINGLE)
            .orientation(Orientation.UNDIRECTED)
            .allowSelfLoops(RandomGraphGeneratorConfig.AllowSelfLoops.NO)
            .build()
            .generate();

        assertThat(graph.availableNodeLabels()).containsExactlyInAnyOrder(NodeLabel.of("LABEL1"),
            NodeLabel.of("LABEL2")
        );
        var filteredIdMap = graph.withFilteredLabels(List.of(NodeLabel.of("LABEL1")), 4).get();
        assertThat(filteredIdMap.nodeCount()).isEqualTo(50);
    }

    static Stream<Arguments> expectedSchemas() {
        return Stream.of(Arguments.of("default",
            RandomGraphGenerator
                .builder()
                .nodeCount(10)
                .averageDegree(1)
                .relationshipDistribution(RelationshipDistribution.RANDOM)
                .build(),
            NodeSchema.empty().addLabel(NodeLabel.ALL_NODES),
            RelationshipSchema.empty().addRelationshipType(RelationshipType.of("REL"), NATURAL)
        ), Arguments.of("node label",
            RandomGraphGenerator
                .builder()
                .nodeCount(10)
                .averageDegree(1)
                .nodeLabelProducer(nodeId -> NodeLabelTokens.ofNodeLabels(NodeLabel.of("A")))
                .relationshipDistribution(RelationshipDistribution.RANDOM)
                .build(),
            NodeSchema.empty().addLabel(NodeLabel.of("A")),
            RelationshipSchema.empty().addRelationshipType(RelationshipType.of("REL"), NATURAL)
        ), Arguments.of("relationship type",
            RandomGraphGenerator
                .builder()
                .nodeCount(10)
                .averageDegree(1)
                .relationshipType(RelationshipType.of("FOOBAR"))
                .relationshipDistribution(RelationshipDistribution.RANDOM)
                .build(),
            NodeSchema.empty().addLabel(NodeLabel.ALL_NODES),
            RelationshipSchema.empty().addRelationshipType(RelationshipType.of("FOOBAR"), NATURAL)
        ), Arguments.of("node label and relationship type",
            RandomGraphGenerator
                .builder()
                .nodeCount(10)
                .averageDegree(1)
                .nodeLabelProducer(nodeId -> NodeLabelTokens.ofNodeLabels(NodeLabel.of("A")))
                .relationshipType(RelationshipType.of("FOOBAR"))
                .relationshipDistribution(RelationshipDistribution.RANDOM)
                .build(),
            NodeSchema.empty().addLabel(NodeLabel.of("A")),
            RelationshipSchema.empty().addRelationshipType(RelationshipType.of("FOOBAR"), NATURAL)
        ), Arguments.of("node label and node property",
            RandomGraphGenerator
                .builder()
                .nodeCount(10)
                .averageDegree(1)
                .nodeLabelProducer(nodeId -> NodeLabelTokens.ofNodeLabels(NodeLabel.of("A")))
                .nodePropertyProducer(PropertyProducer.randomLong("nodeProp", 0, 42))
                .relationshipDistribution(RelationshipDistribution.RANDOM)
                .build(),
            NodeSchema
                .empty()
                .addLabel(NodeLabel.of("A"))
                .addProperty(NodeLabel.of("A"), "nodeProp", ValueType.LONG),
            RelationshipSchema.empty().addRelationshipType(RelationshipType.of("REL"), NATURAL)
        ), Arguments.of("relationship type and node property",
            RandomGraphGenerator
                .builder()
                .nodeCount(10)
                .averageDegree(1)
                .relationshipType(RelationshipType.of("FOOBAR"))
                .relationshipPropertyProducer(PropertyProducer.randomDouble("relProperty", 0, 42))
                .relationshipDistribution(RelationshipDistribution.RANDOM)
                .build(),
            NodeSchema.empty().addLabel(NodeLabel.ALL_NODES),
            RelationshipSchema
                .empty()
                .addProperty(RelationshipType.of("FOOBAR"), NATURAL, "relProperty", ValueType.DOUBLE)
        ), Arguments.of("node label, node property, relationship type and relationship property",
            RandomGraphGenerator
                .builder()
                .nodeCount(10)
                .averageDegree(1)
                .nodeLabelProducer(nodeId -> NodeLabelTokens.ofNodeLabels(NodeLabel.of("A")))
                .nodePropertyProducer(PropertyProducer.randomLong("nodeProp", 0, 42))
                .relationshipType(RelationshipType.of("FOOBAR"))
                .relationshipPropertyProducer(PropertyProducer.randomDouble("relProp", 0, 42))
                .relationshipDistribution(RelationshipDistribution.RANDOM)
                .build(),
            NodeSchema
                .empty()
                .addLabel(NodeLabel.of("A"))
                .addProperty(NodeLabel.of("A"), "nodeProp", ValueType.LONG),
            RelationshipSchema
                .empty()
                .addProperty(RelationshipType.of("FOOBAR"), NATURAL, "relProp", ValueType.DOUBLE)
        ), Arguments.of("relationship type, relationship property, and undirected orientation",
            RandomGraphGenerator
                .builder()
                .nodeCount(10)
                .averageDegree(1)
                .nodeLabelProducer(nodeId -> NodeLabelTokens.ofNodeLabels(NodeLabel.of("A")))
                .relationshipType(RelationshipType.of("FOOBAR"))
                .relationshipPropertyProducer(PropertyProducer.randomDouble("relProp", 0, 42))
                .relationshipDistribution(RelationshipDistribution.RANDOM)
                .orientation(Orientation.UNDIRECTED)
                .build(),
            NodeSchema.empty().addLabel(NodeLabel.of("A")),
            RelationshipSchema
                .empty()
                .addProperty(RelationshipType.of("FOOBAR"), UNDIRECTED, "relProp", ValueType.DOUBLE)
        ));
    }
}
