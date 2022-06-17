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

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import static org.assertj.core.api.Assertions.assertThat;

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

    @GdlGraph(graphNamePrefix = "float")
    private static final String floatQuery =
        "CREATE" +
        "  (a {  kmeans: [1.0f, 1.0f]} )" +
        "  (b {  kmeans: [1.0f, 2.0f]} )" +
        "  (c {  kmeans: [102.0f, 100.0f]} )" +
        "  (d {  kmeans: [100.0f, 102.0f]} )";
    @Inject
    private Graph floatGraph;

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
        var kmeansConfig = ImmutableKmeansStreamConfig.builder()
            .nodeProperty("kmeans")
            .concurrency(1)
            .randomSeed(19L)
            .k(2)
            .build();
        var kmeansContext = ImmutableKmeansContext.builder().build();

        var kmeans = Kmeans.createKmeans(graph, kmeansConfig, kmeansContext);
        var result = kmeans.compute();
        var communities = result.communities();
        var distances = result.distanceFromCenter();
        var centers = result.centers();

        var averageDistance = result.averageDistanceToCentroid();

        assertThat(communities.get(0)).isEqualTo(communities.get(1));
        assertThat(communities.get(2)).isEqualTo(communities.get(3));
        assertThat(communities.get(0)).isNotEqualTo(communities.get(2));

        assertThat(distances.get(0)).isEqualTo(0.5);  //centre is (1,1.5)  sqrt ( 0^2 + 0.5^2)
        assertThat(distances.get(1)).isEqualTo(0.5);
        assertThat(distances.get(2)).isCloseTo(Math.sqrt(2), Offset.offset(1e-4)); //centre is (101,101) sqrt (1^2+1^2)
        assertThat(distances.get(3)).isCloseTo(Math.sqrt(2), Offset.offset(1e-4));

        assertThat(centers[0]).isEqualTo(new double[]{1.0, 1.5});
        assertThat(centers[1]).isEqualTo(new double[]{101, 101});

        //distances are  anyway checked above, so we just take their mean as one last confirmation
        assertThat(averageDistance).isCloseTo(distances.stream().sum() / 4.0, Offset.offset(1e-4));
    }

    @Test
    void shouldRunOnFloatGraph() {
        var kmeansConfig = ImmutableKmeansStreamConfig.builder()
            .nodeProperty("kmeans")
            .concurrency(1)
            .randomSeed(19L)
            .k(2)
            .build();
        var kmeansContext = ImmutableKmeansContext.builder().build();

        var kmeans = Kmeans.createKmeans(floatGraph, kmeansConfig, kmeansContext);
        var result = kmeans.compute();
        var communities = result.communities();
        var centers = result.centers();
        assertThat(communities.get(0)).isEqualTo(communities.get(1));
        assertThat(communities.get(2)).isEqualTo(communities.get(3));
        assertThat(communities.get(0)).isNotEqualTo(communities.get(2));

        assertThat(centers[0]).isEqualTo(new double[]{1.0, 1.5});
        assertThat(centers[1]).isEqualTo(new double[]{101, 101});

    }

    @Test
    void shouldWorkOnLineGraphWithOneIteration() {
        var kmeansConfig = ImmutableKmeansStreamConfig.builder()
            .nodeProperty("kmeans")
            .concurrency(1)
            .randomSeed(19L)
            .k(2)
            .maxIterations(1)
            .build();
        var kmeansContext = ImmutableKmeansContext.builder().build();

        var kmeans = Kmeans.createKmeans(lineGraph, kmeansConfig, kmeansContext);
        var result = kmeans.compute();
        var communities = result.communities();
        assertThat(communities.get(0)).isEqualTo(communities.get(1));
        assertThat(communities.get(2)).isEqualTo(communities.get(3)).isEqualTo(communities.get(4));
        assertThat(communities.get(0)).isNotEqualTo(communities.get(2));
    }

    @Test
    void shouldChangeOnLineGraphWithTwoIterations() {
        var kmeansConfig = ImmutableKmeansStreamConfig.builder()
            .nodeProperty("kmeans")
            .concurrency(1)
            .randomSeed(19L) //init clusters 0.21 and 3.8
            .k(2)
            .maxIterations(2)
            .build();
        var kmeansContext = ImmutableKmeansContext.builder().build();

        var kmeans = Kmeans.createKmeans(lineGraph, kmeansConfig, kmeansContext);
        var result = kmeans.compute();
        var communities = result.communities();

        assertThat(communities.get(1))
            .isEqualTo(communities.get(2))
            .isEqualTo(communities.get(3))
            .isEqualTo(communities.get(4));
        assertThat(communities.get(0)).isNotEqualTo(communities.get(1));
    }
}
