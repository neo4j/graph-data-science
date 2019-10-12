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

import java.util.function.DoubleUnaryOperator;

public abstract class CentralityResultWithStatistics extends CentralityResult {

    protected CentralityResult result;

    CentralityResultWithStatistics(CentralityResult result) {
        super(result.array());
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
            return new HugeDoubleArrayResultWithStatistics(result);
        }
    }

    static final class HugeDoubleArrayResultWithStatistics extends CentralityResultWithStatistics {

        HugeDoubleArrayResultWithStatistics(CentralityResult result) {
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
