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
package org.neo4j.gds.influenceMaximization;

import org.neo4j.gds.annotation.Parameters;

import java.util.Optional;

@Parameters
public class CELFParameters {

    private final  int seedSetSize;

    private final  double propagationProbability;

    private final  int monteCarloSimulations;

    private final  int concurrency;

    private final long randomSeed;

    private final int batchSize;

    public CELFParameters(int seedSetSize, double propagationProbability, int monteCarloSimulations,int concurrency, long randomSeed, int batchSize) {
        this.seedSetSize = seedSetSize;
        this.propagationProbability = propagationProbability;
        this.monteCarloSimulations = monteCarloSimulations;
        this.concurrency = concurrency;
        this.randomSeed = randomSeed;
        this.batchSize=batchSize;
    }

    public static CELFParameters create(
        int seedSetSize,
        double propagationProbability,
        int monteCarloSimulations,
        int concurrency,
        Optional<Long> randomSeed,
        int batchSize

    ) {
        return new CELFParameters(seedSetSize, propagationProbability, monteCarloSimulations,concurrency,randomSeed.orElse(0L),batchSize);
    }

    int concurrency() {
        return concurrency;
    }

    int seedSetSize(){return seedSetSize;}

    double propagationProbability(){return propagationProbability;}

    int monteCarloSimulations() {
        return monteCarloSimulations;
    }

    long randomSeed(){
        return  randomSeed;
    }

    int batchSize(){
        return  batchSize;
    }


}
