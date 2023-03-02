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
package org.neo4j.gds.similarity.knn.metrics;

import com.carrotsearch.hppc.FloatArrayList;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.From;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.Positive;
import org.eclipse.collections.api.tuple.primitive.LongLongPair;
import org.eclipse.collections.impl.tuple.primitive.PrimitiveTuples;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.properties.nodes.LongArrayNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.huge.DirectIdMap;
import org.neo4j.gds.nodeproperties.DoubleArrayTestPropertyValues;
import org.neo4j.gds.nodeproperties.DoubleTestPropertyValues;
import org.neo4j.gds.nodeproperties.FloatArrayTestPropertyValues;
import org.neo4j.gds.nodeproperties.LongArrayTestPropertyValues;
import org.neo4j.gds.nodeproperties.LongTestPropertyValues;

import java.util.Random;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

class SimilarityComputerTest {

    @Property
    void doublePropertySimilarityReturns1ForEqualValues(@ForAll @Positive long id) {
        NodePropertyValues props = new DoubleTestPropertyValues(nodeId -> Math.exp(Math.log1p(nodeId / 42.0)));
        var sim = SimilarityComputer.ofDoubleProperty(props);
        assertThat(sim.similarity(id, id)).isEqualTo(1.0);
    }

    @Property
    void doublePropertySimilarityReturnsValuesBetween0And1(@ForAll @From("differentValues") LongLongPair ids) {
        NodePropertyValues props = new DoubleTestPropertyValues(nodeId -> Math.exp(Math.log1p(nodeId / 42.0)));
        var sim = SimilarityComputer.ofDoubleProperty(props);
        assertThat(sim.similarity(ids.getOne(), ids.getTwo())).isBetween(0.0, 1.0);
    }

    @Property
    void longPropertySimilarityReturns1ForEqualValues(@ForAll @Positive long id) {
        NodePropertyValues props = new LongTestPropertyValues(nodeId -> nodeId);
        var sim = SimilarityComputer.ofLongProperty(props);
        assertThat(sim.similarity(id, id)).isEqualTo(1.0);
    }

    @Property
    void longPropertySimilarityReturnsValuesBetween0And1(@ForAll @From("differentValues") LongLongPair ids) {
        NodePropertyValues props = new LongTestPropertyValues(nodeId -> nodeId);
        var sim = SimilarityComputer.ofLongProperty(props);
        assertThat(sim.similarity(ids.getOne(), ids.getTwo())).isStrictlyBetween(0.0, 1.0);
    }

    @Property
    void floatArrayPropertySimilarityReturns1ForEqualValues(
        @ForAll @Positive long id,
        @ForAll @From("floatArrayMetrics") SimilarityMetric similarityMetric
    ) {
        NodePropertyValues props = new FloatArrayTestPropertyValues(nodeId -> new Random(nodeId).doubles(42, 0.0, 1.0)
            .boxed()
            .reduce(new FloatArrayList(42), (floats, value) -> {
                floats.add(value.floatValue());
                return floats;
            }, (f1, f2) -> f1)
            .toArray()
        );
        var sim = SimilarityComputer.ofFloatArrayProperty("", props, similarityMetric);

        assertThat(sim.similarity(id, id)).isEqualTo(1.0);
    }

    @Property
    void floatArrayPropertySimilarityReturnsValuesBetween0And1(
        @ForAll @From("differentValues") LongLongPair ids,
        @ForAll @From("floatArrayMetrics") SimilarityMetric similarityMetric
    ) {
        NodePropertyValues props = new FloatArrayTestPropertyValues(nodeId -> new Random(nodeId).doubles(42, 0.0, 1.0)
            .boxed()
            .reduce(new FloatArrayList(42), (floats, value) -> {
                floats.add(value.floatValue());
                return floats;
            }, (f1, f2) -> f1)
            .toArray()
        );
        var sim = SimilarityComputer.ofFloatArrayProperty("", props, similarityMetric);
        assertThat(sim.similarity(ids.getOne(), ids.getTwo())).isBetween(0.0, 1.0);
    }

    @Property
    void floatArrayPropertySimilarityReturns0ForNegativeValues(
        @ForAll @From("differentValues") LongLongPair ids,
        @ForAll @From("floatArrayMetrics") SimilarityMetric similarityMetric
    ) {
        NodePropertyValues props = new FloatArrayTestPropertyValues(nodeId -> new Random(nodeId)
            .doubles(42, -10.0, 10.0)
            .boxed()
            .reduce(new FloatArrayList(42), (floats, value) -> {
                floats.add(value.floatValue());
                return floats;
            }, (f1, f2) -> f1)
            .toArray());

        var sim = SimilarityComputer.ofFloatArrayProperty("", props, similarityMetric);
        assertThat(sim.similarity(ids.getOne(), ids.getTwo())).isBetween(0.0, 1.0);
    }

    @Property
    void doubleArrayPropertySimilarityReturns1ForEqualValues(
        @ForAll @Positive long id,
        @ForAll @From("doubleArrayMetrics") SimilarityMetric similarityMetric
    ) {
        NodePropertyValues props = new DoubleArrayTestPropertyValues(nodeId -> new Random(nodeId).doubles(42, 0.0, 1.0).toArray());
        var sim = SimilarityComputer.ofDoubleArrayProperty("", props, similarityMetric);

        assertThat(sim.similarity(id, id)).isEqualTo(1.0);
    }

    @Property
    void doubleArrayPropertySimilarityReturnsValuesBetween0And1(
        @ForAll @From("differentValues") LongLongPair ids,
        @ForAll @From("doubleArrayMetrics") SimilarityMetric similarityMetric
    ) {
        NodePropertyValues props = new DoubleArrayTestPropertyValues(nodeId -> new Random(nodeId).doubles(42, 0.0, 1.0).toArray());
        var sim = SimilarityComputer.ofDoubleArrayProperty("", props, similarityMetric);
        assertThat(sim.similarity(ids.getOne(), ids.getTwo())).isBetween(0.0, 1.0);
    }

    @Property
    void doubleArrayPropertySimilarityReturns0ForNegativeValues(
        @ForAll @From("differentValues") LongLongPair ids,
        @ForAll @From("doubleArrayMetrics") SimilarityMetric similarityMetric
    ) {
        NodePropertyValues props = new DoubleArrayTestPropertyValues(nodeId -> new Random(nodeId).doubles(42, -10.0, 10.0).toArray());

        var sim = SimilarityComputer.ofDoubleArrayProperty("", props, similarityMetric);

        assertThat(sim.similarity(ids.getOne(), ids.getTwo())).isBetween(0.0, 1.0);
    }

    @Property
    void longArrayPropertySimilarityReturns1ForEqualValues(
        @ForAll @Positive long id,
        @ForAll @From("longArrayMetrics") SimilarityMetric similarityMetric
    ) {
        NodePropertyValues props = new LongArrayTestPropertyValues(nodeId -> new Random(nodeId).longs(42, 0, 1337).toArray());
        var sim = SimilarityComputer.ofLongArrayProperty("", props, similarityMetric);

        assertThat(sim.similarity(id, id)).isEqualTo(1.0);
    }

    @Property
    void longArrayPropertySimilarityReturnsValuesBetween0And1(
        @ForAll @From("differentValues") LongLongPair ids,
        @ForAll @From("longArrayMetrics") SimilarityMetric similarityMetric
    ) {
        NodePropertyValues props = new LongArrayTestPropertyValues(nodeId -> new Random(nodeId).longs(42, 0, 1337).toArray());
        var sim = SimilarityComputer.ofLongArrayProperty("", props, similarityMetric);
        assertThat(sim.similarity(ids.getOne(), ids.getTwo())).isBetween(0.0, 1.0);
    }

    @Property
    void longArrayPropertySimilarityReturns0ForNegativeValues(
        @ForAll @From("differentValues") LongLongPair ids,
        @ForAll @From("longArrayMetrics") SimilarityMetric similarityMetric
    ) {
        NodePropertyValues props = new LongArrayTestPropertyValues(nodeId -> new Random(nodeId).longs(42, -10, 10).toArray());
        var sim = SimilarityComputer.ofLongArrayProperty("", props, similarityMetric);

        assertThat(sim.similarity(ids.getOne(), ids.getTwo())).isBetween(0.0, 1.0);
    }

    @Property
    void similarVectorsShouldBeCLoseToOne() {
        double[] doubleArray = new Random(42).doubles(42, 0.0, 1.0).toArray();
        double[] doubleArrayCopy = doubleArray.clone();
        doubleArrayCopy[1] = doubleArrayCopy[1] + 1.0D;
        NodePropertyValues props = new DoubleArrayTestPropertyValues(nodeId -> {
            if (nodeId == 0) {
                return doubleArray;
            }
            if (nodeId == 1) {
                return doubleArrayCopy;
            }
            return new double[0];
        });
        var sim = SimilarityComputer.ofDoubleArrayProperty(
            "",
            props,
            SimilarityMetric.defaultMetricForType(ValueType.DOUBLE_ARRAY)
        );

        assertThat(sim.similarity(0, 1)).isCloseTo(1.0D, within(0.05));
    }

    @ParameterizedTest
    @MethodSource("nonFiniteSimilarities")
    void safeSimilaritySwallowsNonFiniteValues(SimilarityComputer sim) {
        assertThat(sim.similarity(42, 1337)).isNaN();
        assertThat(sim.safeSimilarity(42, 1337)).isZero();
    }

    @Test
    void doubleArraySimilarityComputerHandlesNullProperties() {
        NodePropertyValues props = new DoubleArrayTestPropertyValues(nodeId -> null);
        var sim = SimilarityComputer.ofProperty(
            new DirectIdMap(2),
            "doubleArrayProperty",
            props,
            SimilarityMetric.defaultMetricForType(ValueType.DOUBLE_ARRAY)
        );
        assertThatThrownBy(() -> sim.similarity(0, 1))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Missing `List of Float` node property `doubleArrayProperty` for node with id");
    }

    @Test
    void floatArraySimilarityComputerHandlesNullProperties() {
        NodePropertyValues props = new FloatArrayTestPropertyValues(nodeId -> null);
        var sim = SimilarityComputer.ofProperty(
            new DirectIdMap(2),
            "floatArrayProperty",
            props,
            SimilarityMetric.defaultMetricForType(ValueType.FLOAT_ARRAY)
        );
        assertThatThrownBy(() -> sim.similarity(0, 1))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Missing `List of Float` node property `floatArrayProperty` for node with id");
    }

    @Test
    void longArraySimilarityComputerHandlesNullProperties() {
        var nodeCount = 2;
        NodePropertyValues props = new LongArrayNodePropertyValues() {
            @Override
            public long[] longArrayValue(long nodeId) {
                return null;
            }

            @Override
            public long valuesStored() {
                return 0;
            }

            @Override
            public long maxIndex() {
                return nodeCount;
            }
        };
        var idMap = new DirectIdMap(nodeCount);
        assertThatThrownBy(() ->
            SimilarityComputer.ofProperty(
                idMap,
                "longArrayProperty",
                props,
                SimilarityMetric.defaultMetricForType(ValueType.LONG_ARRAY)
            )).isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Missing `List of Integer` node property `longArrayProperty` for node with id");
    }

    static Stream<SimilarityComputer> nonFiniteSimilarities() {
        return Stream.of(
            SimilarityComputer.ofDoubleProperty(new DoubleTestPropertyValues(nodeId -> Double.NaN)),
            SimilarityComputer.ofDoubleProperty(new DoubleTestPropertyValues(nodeId -> Double.POSITIVE_INFINITY)),
            SimilarityComputer.ofDoubleProperty(new DoubleTestPropertyValues(nodeId -> Double.NEGATIVE_INFINITY)),
            SimilarityComputer.ofFloatArrayProperty(
                "",
                new FloatArrayTestPropertyValues(nodeId -> new float[]{}),
                SimilarityMetric.defaultMetricForType(ValueType.FLOAT_ARRAY)
            ),
            SimilarityComputer.ofDoubleArrayProperty(
                "",
                new DoubleArrayTestPropertyValues(nodeId -> new double[]{}),
                SimilarityMetric.defaultMetricForType(ValueType.DOUBLE_ARRAY)
            )
        );
    }

    @Provide("differentValues")
    final Arbitrary<LongLongPair> differentValues() {
        return Arbitraries.longs().between(0L, Long.MAX_VALUE).flatMap(n1 -> Arbitraries.longs().between(0L, Long.MAX_VALUE)
            .filter(n2 -> n1.longValue() != n2.longValue())
            .map(n2 -> PrimitiveTuples.pair((long) n1, (long) n2)));
    }

    @Provide("longArrayMetrics")
    final Arbitrary<SimilarityMetric> longArrayMetrics() {
        return Arbitraries.of(SimilarityMetric.JACCARD, SimilarityMetric.OVERLAP);
    }

    @Provide("doubleArrayMetrics")
    final Arbitrary<SimilarityMetric> doubleArrayMetrics() {
        return Arbitraries.of(SimilarityMetric.COSINE, SimilarityMetric.EUCLIDEAN, SimilarityMetric.PEARSON);
    }

    @Provide("floatArrayMetrics")
    final Arbitrary<SimilarityMetric> floatArrayMetrics() {
        return Arbitraries.of(SimilarityMetric.COSINE, SimilarityMetric.EUCLIDEAN, SimilarityMetric.PEARSON);
    }
}
