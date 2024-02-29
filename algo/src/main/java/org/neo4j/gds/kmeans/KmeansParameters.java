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

import org.neo4j.gds.annotation.Parameters;

import java.util.List;
import java.util.Optional;

@Parameters
public class KmeansParameters  {

    private final int k;
    private final int maxIterations;
    private final double deltaThreshold;
    private final int numberOfRestarts;
    private final  boolean computeSilhouette;
    private final int concurrency;
    private final  String  nodeProperty;
    private final KmeansSampler.SamplerType samplerType;
    private final List<List<Double>> seedCentroids;
    private final Optional<Long> randomSeed;

    public KmeansParameters(
        int k,
        int maxIterations,
        double deltaThreshold,
        int numberOfRestarts,
        boolean computeSilhouette,
        int concurrency,
        String nodeProperty,
        KmeansSampler.SamplerType samplerType,
        List<List<Double>> seedCentroids,
        Optional<Long> randomSeed
    ) {
        this.k = k;
        this.maxIterations = maxIterations;
        this.deltaThreshold = deltaThreshold;
        this.numberOfRestarts = numberOfRestarts;
        this.computeSilhouette = computeSilhouette;
        this.concurrency = concurrency;
        this.nodeProperty = nodeProperty;
        this.samplerType = samplerType;
        this.seedCentroids = seedCentroids;
        this.randomSeed = randomSeed;
    }

    public  int k(){
        return  k;
    }

    public  int maxIterations(){
        return  maxIterations;
    }

    public  double deltaThreshold(){
        return  deltaThreshold;
    }

    public  int numberOfRestarts(){
        return  numberOfRestarts;
    }

    public  boolean computeSilhouette(){
        return  computeSilhouette;
    }

    public int concurrency(){
        return concurrency;
    }

    public String nodeProperty(){
        return  nodeProperty;
    }

    public Optional<Long> randomSeed(){
        return  randomSeed;
    }

    public KmeansSampler.SamplerType samplerType(){
        return samplerType;
    }

    public List<List<Double>> seedCentroids(){
        return  seedCentroids;
    }

    public boolean isSeeded() {
        return !seedCentroids().isEmpty();
    }

}
