/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
import org.neo4j.graphalgo.core.write.Translators;

import java.util.Arrays;
import java.util.function.Function;

public final class DoubleArrayResult implements CentralityResult {
    private final double[] result;

    public DoubleArrayResult(double[] result) {
        this.result = result;
    }

    @Override
    public void export(
            final String propertyName, final Exporter exporter) {
        exporter.write(
                propertyName,
                result,
                Translators.DOUBLE_ARRAY_TRANSLATOR);
    }

    @Override
    public void export(String propertyName, Exporter exporter, Function<Double, Double> normalizationFunction) {
        exporter.write(
                propertyName,
                Arrays.stream(result).map(normalizationFunction::apply).toArray(),
                Translators.DOUBLE_ARRAY_TRANSLATOR);
    }

    @Override
    public double computeMax() {
        return NormalizationComputations.max(result, 1.0);
    }

    @Override
    public double computeL2Norm() {
        return Math.sqrt(NormalizationComputations.squaredSum(result));
    }

    @Override
    public double computeL1Norm() {
        return NormalizationComputations.l1Norm(result);
    }

    @Override
    public final double score(final long nodeId) {
        return result[(int) nodeId];
    }

    @Override
    public double score(final int nodeId) {
        return result[nodeId];
    }
}
