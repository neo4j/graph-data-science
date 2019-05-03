/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.impl.results;

import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.impl.CentralityUtils;

import java.util.function.Function;

import static org.neo4j.graphalgo.core.utils.ArrayUtil.binaryLookup;

public final class PartitionedDoubleArrayResult implements CentralityResult, PropertyTranslator.OfDouble<double[][]> {
    private final double[][] partitions;
    private final long[] starts;

    public PartitionedDoubleArrayResult(
            double[][] partitions,
            long[] starts) {
        this.partitions = partitions;
        this.starts = starts;
    }

    @Override
    public void export(final String propertyName, final Exporter exporter) {
        exporter.write(propertyName, partitions, this);
    }


    @Override
    public void export(String propertyName, Exporter exporter, Function<Double, Double> normalizationFunction) {
        CentralityUtils.normalizeArray(partitions, normalizationFunction);
        export(propertyName, exporter);
    }

    @Override
    public double computeMax() {
        return NormalizationComputations.max(partitions);
    }

    @Override
    public double computeL2Norm() {
        return NormalizationComputations.l2Norm(partitions);
    }

    @Override
    public double computeL1Norm() {
        return NormalizationComputations.l1Norm(partitions);
    }

    @Override
    public double toDouble(final double[][] data, final long nodeId) {
        int idx = binaryLookup(nodeId, starts);
        return data[idx][(int) (nodeId - starts[idx])];
    }

    @Override
    public double score(final long nodeId) {
        return toDouble(partitions, nodeId);
    }

    @Override
    public double score(final int nodeId) {
        return score((long) nodeId);
    }
}
