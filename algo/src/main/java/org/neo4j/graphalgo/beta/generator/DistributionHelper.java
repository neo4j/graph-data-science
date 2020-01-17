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
package org.neo4j.graphalgo.beta.generator;

import java.util.Random;

final class DistributionHelper {
    private DistributionHelper() {}

    static long uniformSample(long upperBound, Random random) {
        // bitwise AND to make it positive
        return (random.nextLong() & Long.MAX_VALUE) % upperBound;
    }

    static long gauseanSample(long upperBound, long mean, long stdDev, Random random) {
        double gaussian = random.nextGaussian() * stdDev + mean % upperBound;
        return Math.round(gaussian);
    }

    // https://stackoverflow.com/questions/17882907/python-scipy-stats-powerlaw-negative-exponent/46065079#46065079
    static long powerLawSample(long min, long max, double gamma, Random random) {
        double v = Math.pow((Math.pow(max, -gamma + 1.0d) - Math.pow(min, -gamma + 1.0d) * random.nextDouble() + Math.pow(min, -gamma + 1.0d)), 1.0d / (-gamma + 1.0d));
        return Math.round(v);
    }
}
