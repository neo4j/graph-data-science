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

package org.neo4j.graphalgo.results;

import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.impl.results.CentralityResult;
import org.neo4j.graphalgo.impl.results.HugeDoubleArrayResult;
import org.neo4j.graphalgo.impl.results.PartitionedDoubleArrayResult;

import java.util.function.DoubleUnaryOperator;

public abstract class CentralityResultWithStatistics<T extends CentralityResult> implements CentralityResult {

    protected T result;

    CentralityResultWithStatistics(T result) {
        this.result = result;
    }

    public abstract double computeMax();

    public abstract double computeL2Norm();

    public abstract double computeL1Norm();

    @Override
    public double score(int nodeId) {
        return result.score(nodeId);
    }

    @Override
    public double score(long nodeId) {
        return result.score(nodeId);
    }

    @Override
    public void export(String propertyName, Exporter exporter) {
        result.export(propertyName, exporter);
    }

    @Override
    public void export(String propertyName, Exporter exporter, DoubleUnaryOperator normalizationFunction) {
        result.export(propertyName, exporter, normalizationFunction);
    }

    public static final class Builder {
        private Builder() {}

        public static CentralityResultWithStatistics of(CentralityResult result) {
            if (result instanceof PartitionedDoubleArrayResult) {
                return new PartitionedCentralityResultsWithStatistics((PartitionedDoubleArrayResult) result);
            } else if (result instanceof HugeDoubleArrayResult) {
                return new HugeDoubleArrayResultWithStatistics((HugeDoubleArrayResult) result);
            } else {
                throw new IllegalArgumentException(String.format(
                        "Unsupported CentralityResult %s.",
                        result.getClass().getSimpleName()));
            }
        }
    }

    static final class PartitionedCentralityResultsWithStatistics extends CentralityResultWithStatistics<PartitionedDoubleArrayResult> {

        PartitionedCentralityResultsWithStatistics(PartitionedDoubleArrayResult result) {
            super(result);
        }

        @Override
        public double computeMax() {
            return NormalizationComputations.max(result.partitions());
        }

        @Override
        public double computeL2Norm() {
            return NormalizationComputations.l2Norm(result.partitions());
        }

        @Override
        public double computeL1Norm() {
            return NormalizationComputations.l1Norm(result.partitions());
        }
    }

    static final class HugeDoubleArrayResultWithStatistics extends CentralityResultWithStatistics<HugeDoubleArrayResult> {

        HugeDoubleArrayResultWithStatistics(HugeDoubleArrayResult result) {
            super(result);
        }

        @Override
        public double computeMax() {
            return HugeNormalizationComputations.max(result.array(), 1.0);
        }

        @Override
        public double computeL2Norm() {
            return Math.sqrt(HugeNormalizationComputations.squaredSum(result.array()));
        }

        @Override
        public double computeL1Norm() {
            return HugeNormalizationComputations.l1Norm(result.array());
        }
    }
}
