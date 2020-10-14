/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.similarity.knn;

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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.nodeproperties.DoubleArrayNodeProperties;
import org.neo4j.graphalgo.api.nodeproperties.DoubleNodeProperties;
import org.neo4j.graphalgo.api.nodeproperties.FloatArrayNodeProperties;
import org.neo4j.graphalgo.api.nodeproperties.LongArrayNodeProperties;
import org.neo4j.graphalgo.api.nodeproperties.LongNodeProperties;

import java.util.Random;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class SimilarityComputerTest {

    @Property
    void defaultIdSimilarityReturns1ForEqualValues(@ForAll @Positive long id) {
        var sim = SimilarityComputer.DEFAULT_SIMILARITY_COMPUTER;
        assertThat(sim.similarity(id, id)).isEqualTo(1.0);
    }

    @Property
    void defaultIdSimilarityReturnsValuesBetween0And1(@ForAll @From("differentValues") LongLongPair ids) {
        var sim = SimilarityComputer.DEFAULT_SIMILARITY_COMPUTER;
        assertThat(sim.similarity(ids.getOne(), ids.getTwo())).isStrictlyBetween(0.0, 1.0);
    }

    @Property
    void doublePropertySimilarityReturns1ForEqualValues(@ForAll @Positive long id) {
        NodeProperties props = (DoubleNodeProperties) nodeId -> Math.exp(Math.log1p(nodeId / 42.0));
        var sim = SimilarityComputer.ofDoubleProperty(props);
        assertThat(sim.similarity(id, id)).isEqualTo(1.0);
    }

    @Property
    void doublePropertySimilarityReturnsValuesBetween0And1(@ForAll @From("differentValues") LongLongPair ids) {
        NodeProperties props = (DoubleNodeProperties) nodeId -> Math.exp(Math.log1p(nodeId / 42.0));
        var sim = SimilarityComputer.ofDoubleProperty(props);
        assertThat(sim.similarity(ids.getOne(), ids.getTwo())).isStrictlyBetween(0.0, 1.0);
    }

    @Property
    void longPropertySimilarityReturns1ForEqualValues(@ForAll @Positive long id) {
        NodeProperties props = (LongNodeProperties) nodeId -> nodeId;
        var sim = SimilarityComputer.ofLongProperty(props);
        assertThat(sim.similarity(id, id)).isEqualTo(1.0);
    }

    @Property
    void longPropertySimilarityReturnsValuesBetween0And1(@ForAll @From("differentValues") LongLongPair ids) {
        NodeProperties props = (LongNodeProperties) nodeId -> nodeId;
        var sim = SimilarityComputer.ofLongProperty(props);
        assertThat(sim.similarity(ids.getOne(), ids.getTwo())).isStrictlyBetween(0.0, 1.0);
    }

    @Property
    void floatArrayPropertySimilarityReturns1ForEqualValues(@ForAll @Positive long id) {
        NodeProperties props = (FloatArrayNodeProperties) nodeId -> new Random(nodeId).doubles(42, 0.0, 1.0)
            .boxed()
            .reduce(new FloatArrayList(42), (floats, value) -> {
                floats.add(value.floatValue());
                return floats;
            }, (f1, f2) -> f1)
            .toArray();
        var sim = SimilarityComputer.ofFloatArrayProperty(props);

        assertThat(sim.similarity(id, id)).isEqualTo(1.0);
    }

    @Property
    void floatArrayPropertySimilarityReturnsValuesBetween0And1(@ForAll @From("differentValues") LongLongPair ids) {
        NodeProperties props = (FloatArrayNodeProperties) nodeId -> new Random(nodeId).doubles(42, 0.0, 1.0)
            .boxed()
            .reduce(new FloatArrayList(42), (floats, value) -> {
                floats.add(value.floatValue());
                return floats;
            }, (f1, f2) -> f1)
            .toArray();
        var sim = SimilarityComputer.ofFloatArrayProperty(props);
        assertThat(sim.similarity(ids.getOne(), ids.getTwo())).isStrictlyBetween(0.0, 1.0);
    }

    @Property
    void doubleArrayPropertySimilarityReturns1ForEqualValues(@ForAll @Positive long id) {
        NodeProperties props = (DoubleArrayNodeProperties) nodeId -> new Random(nodeId).doubles(42, 0.0, 1.0).toArray();
        var sim = SimilarityComputer.ofDoubleArrayProperty(props);

        assertThat(sim.similarity(id, id)).isEqualTo(1.0);
    }

    @Property
    void doubleArrayPropertySimilarityReturnsValuesBetween0And1(@ForAll @From("differentValues") LongLongPair ids) {
        NodeProperties props = (DoubleArrayNodeProperties) nodeId -> new Random(nodeId).doubles(42, 0.0, 1.0).toArray();
        var sim = SimilarityComputer.ofDoubleArrayProperty(props);
        assertThat(sim.similarity(ids.getOne(), ids.getTwo())).isStrictlyBetween(0.0, 1.0);
    }

    @Property
    void longArrayPropertySimilarityReturns1ForEqualValues(@ForAll @Positive long id) {
        NodeProperties props = (LongArrayNodeProperties) nodeId -> new Random(nodeId).longs(42, 0, 1337).toArray();
        var sim = SimilarityComputer.ofLongArrayProperty(props);

        assertThat(sim.similarity(id, id)).isEqualTo(1.0);
    }

    @Property
    void longArrayPropertySimilarityReturnsValuesBetween0And1(@ForAll @From("differentValues") LongLongPair ids) {
        NodeProperties props = (LongArrayNodeProperties) nodeId -> new Random(nodeId).longs(42, 0, 1337).toArray();
        var sim = SimilarityComputer.ofLongArrayProperty(props);
        assertThat(sim.similarity(ids.getOne(), ids.getTwo())).isStrictlyBetween(0.0, 1.0);
    }

    @Property
    void similarVectorsShouldBeCLoseToOne() {
        double[] doubleArray = new Random(42).doubles(42, 0.0, 1.0).toArray();
        double[] doubleArrayCopy = doubleArray.clone();
        doubleArrayCopy[1] = doubleArrayCopy[1] + 1.0D;
        NodeProperties props = (DoubleArrayNodeProperties) nodeId -> {
            if (nodeId == 0) {
                return doubleArray;
            }
            if (nodeId == 1) {
                return doubleArrayCopy;
            }
            return new double[0];
        };
        var sim = SimilarityComputer.ofDoubleArrayProperty(props);

        assertThat(sim.similarity(0, 1)).isCloseTo(1.0D, within(0.05));
    }

    @ParameterizedTest
    @MethodSource("nonFiniteSimilarities")
    void safeSimilaritySwallowsNonFiniteValues(SimilarityComputer sim) {
        assertThat(sim.similarity(42, 1337)).isNaN();
        assertThat(sim.safeSimilarity(42, 1337)).isZero();
    }

    static Stream<SimilarityComputer> nonFiniteSimilarities() {
        return Stream.of(
            SimilarityComputer.ofDoubleProperty((DoubleNodeProperties) nodeId -> Double.NaN),
            SimilarityComputer.ofDoubleProperty((DoubleNodeProperties) nodeId -> Double.POSITIVE_INFINITY),
            SimilarityComputer.ofDoubleProperty((DoubleNodeProperties) nodeId -> Double.NEGATIVE_INFINITY),
            SimilarityComputer.ofFloatArrayProperty((FloatArrayNodeProperties) nodeId -> new float[]{}),
            SimilarityComputer.ofDoubleArrayProperty((DoubleArrayNodeProperties) nodeId -> new double[]{})
        );
    }

    @Provide("differentValues")
    final Arbitrary<LongLongPair> differentValues() {
        return Arbitraries.longs().between(0L, Long.MAX_VALUE).flatMap(n1 ->
            Arbitraries.longs().between(0L, Long.MAX_VALUE)
                .filter(n2 -> n1.longValue() != n2.longValue())
                .map(n2 -> PrimitiveTuples.pair((long) n1, (long) n2)));
    }
}
