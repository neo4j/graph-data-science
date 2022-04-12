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
package org.neo4j.gds.kmeans;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.nodeproperties.DoubleArrayTestProperties;
import org.neo4j.gds.nodeproperties.FloatArrayTestProperties;
import org.neo4j.gds.nodeproperties.LongTestProperties;

import java.util.SplittableRandom;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@GdlExtension
class KmeansTest {
    @GdlGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a {  kmeans: [1.0, 1.0]} )" +
        "  (b {  kmeans: [1.0, 2.0]} )" +
        "  (c {  kmeans: [102.0, 100.0]} )" +
        "  (d {  kmeans: [100.0, 102.0]} )";
    @Inject
    private Graph graph;

    @GdlGraph(graphNamePrefix = "line")
    private static final String LineQuery =
        "CREATE" +
        "  (a {  kmeans: [0.21, 0.0]} )" +
        "  (b {  kmeans: [2.0, 0.0]} )" +
        "  (c {  kmeans: [2.1, 0.0]} )" +
        "  (d {  kmeans: [3.8, 0.0]} )" +
        "  (e {  kmeans: [2.1, 0.0]} )";

    @Inject
    private TestGraph lineGraph;

    @Inject
    private IdFunction idFunction;

    @Test
    void shouldRun() {
        var kmeansConfig = ImmutableKmeansBaseConfig.builder()
            .nodeWeightProperty("kmeans")
            .concurrency(1)
            .randomSeed(19L)
            .k(2)
            .build();
        var kmeansContext = ImmutableKmeansContext.builder().build();

        var kmeans = Kmeans.createKmeans(graph, kmeansConfig, kmeansContext);
        var result = kmeans.compute().communities();
        assertThat(result.get(0)).isEqualTo(result.get(1));
        assertThat(result.get(2)).isEqualTo(result.get(3));
        assertThat(result.get(0)).isNotEqualTo(result.get(2));
    }

    @Test
    void shouldWorkOnLineGraphWithOneIteration() {
        var kmeansConfig = ImmutableKmeansBaseConfig.builder()
            .nodeWeightProperty("kmeans")
            .concurrency(1)
            .randomSeed(19L)
            .k(2)
            .maxIterations(1)
            .build();
        var kmeansContext = ImmutableKmeansContext.builder().build();

        var kmeans = Kmeans.createKmeans(lineGraph, kmeansConfig, kmeansContext);
        var result = kmeans.compute().communities();
        assertThat(result.get(0)).isEqualTo(result.get(1));
        assertThat(result.get(2)).isEqualTo(result.get(3)).isEqualTo(result.get(4));
        assertThat(result.get(0)).isNotEqualTo(result.get(2));
    }

    @Test
    void shouldChangeOnLineGraphWithTwoIterations() {
        var kmeansConfig = ImmutableKmeansBaseConfig.builder()
            .nodeWeightProperty("kmeans")
            .concurrency(1)
            .randomSeed(19L) //init clusters 0.21 and 3.8
            .k(2)
            .maxIterations(2)
            .build();
        var kmeansContext = ImmutableKmeansContext.builder().build();

        var kmeans = Kmeans.createKmeans(lineGraph, kmeansConfig, kmeansContext);
        var result = kmeans.compute().communities();

        assertThat(result.get(1)).isEqualTo(result.get(2)).isEqualTo(result.get(3)).isEqualTo(result.get(4));
        assertThat(result.get(0)).isNotEqualTo(result.get(1));
    }

    @Test
    void shouldFailOnInvalidPropertyValueTypes() {
        var longProperties = new LongTestProperties(n -> n);
        assertThatThrownBy(() -> new Kmeans(
                ProgressTracker.NULL_TRACKER,
                Pools.DEFAULT,
                graph,
                5,
                4,
                10,
                0.1,
                longProperties,
                new SplittableRandom()
            ).compute()
        ).isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining(
                "Unsupported node property value type [LONG]. Value type required: [DOUBLE_ARRAY] or [FLOAT_ARRAY].");
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.kmeans.KmeansTest#validNodeProperties")
    void shouldAcceptValidPropertyValueTypes(NodeProperties nodeProperties) {
        assertThatNoException()
            .isThrownBy(() -> new Kmeans(
                ProgressTracker.NULL_TRACKER,
                Pools.DEFAULT,
                graph,
                5,
                4,
                10,
                0.1,
                nodeProperties,
                new SplittableRandom()
            ).compute()
        );
    }

    static Stream<Arguments> validNodeProperties() {
        return Stream.of(
            Arguments.of(new DoubleArrayTestProperties(__ -> new double[]{1.0D})),
            Arguments.of(new FloatArrayTestProperties(__ -> new float[]{1.0F}))
        );
    }
}
