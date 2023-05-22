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
import org.neo4j.gds.TestProgressTracker;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;

import java.util.List;
import java.util.SplittableRandom;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class KmeansGridTest {

    // override offset as test involved sampling
    @GdlGraph(idOffset = 0)
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a11 {  kmeans: [1.0, 1.0]} )" +
        "  (a12 {  kmeans: [1.0, 2.0]} )" +
        "  (a13 {  kmeans: [1.0 , 3.0]} )" +
        "  (a21 {  kmeans: [2.0, 1.0]} )" +
        "  (a22 {  kmeans: [2.0, 2.0]} )" +
        "  (a23 {  kmeans: [2.0 , 3.0]} )" +
        "  (a31 {  kmeans: [3.0, 1.0]} )" +
        "  (a32 {  kmeans: [3.0, 2.0]} )" +
        "  (a33 {  kmeans: [3.0 , 3.0]} )" +
        "  (b11 {  kmeans: [-1.0, -1.0]} )" +
        "  (b12 {  kmeans: [-1.0, -2.0]} )" +
        "  (b13 {  kmeans: [-1.0 , -3.0]} )" +
        "  (b21 {  kmeans: [-2.0, -1.0]} )" +
        "  (b22 {  kmeans: [-2.0, -2.0]} )" +
        "  (b23 {  kmeans: [-2.0 , -3.0]} )" +
        "  (b31 {  kmeans: [-3.0, -1.0]} )" +
        "  (b32 {  kmeans: [-3.0, -2.0]} )" +
        "  (b33 {  kmeans: [-3.0 , -3.0]} )";
    @Inject
    private Graph graph;


    @Test
    void calculateInitialCentersCorrectly(){
      //  SplittableRandom  splittableRandom=new SplittableRandom(19);
    //    System.out.println(splittableRandom.nextLong(graph.nodeCount())) gives 0
        //squared distance from 0 is 204 :
        //(1,1) (1,2)  = 1^2 = 1
        //(1,1) (1,3)  = 2^2 = 4
        //(1,1,) (2,1) = 1^2 = 1
        //(1,1) (2,2) = 1^2 + 1^2=2
        //(1,1) (2,3) = 1^2 + 2^2 = 5
        //(1,1) (3,1)  = 2^2 = 4
        //(1,1) (3,2) = 2^2 + 1^2 = 5
        //(1,1) (3,3) = 2^2 + 2^2 = 8
        //(1,1) (-1,-1) = 2^2 + 2^2 = 8
        //(1,1) (-1,-2) = 2^2  +3^2 = 13
        //(1,1) (-1,-3) = 2^2 + 4^2 =20
        //(1,1) (-2,-1) =3^2 + 2^2 =13
        //(1,1) (-2,-2) = 3^2 + 3^2 =18
        //(1,1) (-2,-3) = 3^2 + 4^2 =25
        //(1,1) (-3,-1) = 4^2 + 2^2=20
        //(1,1) (-3,2) = 4^2 + 3^2 =25
        //(1,1) (-3,-3) = 4^2 + 4^2=32
      //  System.out.println(splittableRandom.nextDouble()) * 204  gives appx. 23.3xx
        //thus it should select (3,3) as the second center.

        var kmeansContext = ImmutableKmeansContext.builder().build();

        var nodePropertyValues = graph.nodeProperties("kmeans");

        ClusterManager clusterManager = ClusterManager.createClusterManager(nodePropertyValues, 2, 2);
        HugeIntArray communities = HugeIntArray.newArray(graph.nodeCount());
        HugeDoubleArray distanceFromCentroid = HugeDoubleArray.newArray(graph.nodeCount());
        var tasks = List.of(
            KmeansTask.createTask(
                KmeansSampler.SamplerType.KMEANSPP,
                clusterManager,
                nodePropertyValues,
                communities,
                distanceFromCentroid,
                2,
                2,
                new Partition(0, graph.nodeCount())
            )
        );
        KmeansSampler kmeansSampler = KmeansSampler.createSampler(
            KmeansSampler.SamplerType.KMEANSPP,
            new SplittableRandom(19),
            nodePropertyValues,
            clusterManager,
            graph.nodeCount(),
            2,
            2,
            distanceFromCentroid,
            kmeansContext.executor(),
            tasks,
            TestProgressTracker.NULL_TRACKER
        );
        kmeansSampler.performInitialSampling();
        var centroids= clusterManager.getCentroids();
        var centroid0= centroids[0];
        var centroid1= centroids[1];
        assertThat(centroid0).isEqualTo(new double[]{1.0,1.0});
        assertThat(centroid1).isEqualTo(new double[]{3.0,3.0});


    }
    @Test
    void shouldStabilize(){
        var kmeansConfig = ImmutableKmeansStreamConfig.builder()
            .nodeProperty("kmeans")
            .randomSeed(19L)
            .k(2)
            .maxIterations(3)
            .initialSampler(KmeansSampler.SamplerType.KMEANSPP)
            .build();
        var kmeansContext = ImmutableKmeansContext.builder().build();

        var kmeans = Kmeans.createKmeans(graph, kmeansConfig, kmeansContext);
        var result = kmeans.compute();

        var centroids = result.centers();
        var centroid0=centroids[0];
        var centroid1=centroids[1];

        //negatives grid is one cluster by itself, positive grid is one cluster by itself
        //centroid i the avg of all points i.e., (1+2+3)/3 = 2.0
        assertThat(centroid0).isEqualTo(new double[]{-2.0,-2.0});
        assertThat(centroid1).isEqualTo(new double[]{2.0,2.0});
    }


}
