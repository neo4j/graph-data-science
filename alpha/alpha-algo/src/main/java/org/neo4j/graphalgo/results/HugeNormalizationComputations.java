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
package org.neo4j.graphalgo.results;

import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;

import java.util.stream.DoubleStream;

import static org.neo4j.graphalgo.core.concurrency.ParallelUtil.parallelStream;

public final class HugeNormalizationComputations {

    private HugeNormalizationComputations() {}

    public static double squaredSum(HugeDoubleArray partition, int concurrency) {
        return parallelStream(partition.stream(), concurrency, stream -> stream.map(value -> value * value).sum());
    }

    public static double l1Norm(HugeDoubleArray partition, int concurrency) {
        return parallelStream(partition.stream(), concurrency, DoubleStream::sum);
    }

    public static double max(HugeDoubleArray result, double defaultMax, int concurrency) {
        return parallelStream(result.stream(), concurrency, stream -> stream.max().orElse(defaultMax));
    }
}
