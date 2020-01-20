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
package org.neo4j.graphalgo.impl.utils;

import org.neo4j.graphalgo.results.CentralityResult;
import org.neo4j.graphalgo.results.CentralityResultWithStatistics;
import org.neo4j.graphalgo.results.NormalizedCentralityResult;

public enum NormalizationFunction {
    NONE {
        @Override
        public CentralityResult apply(CentralityResultWithStatistics scores) {
            return scores;
        }
    },
    MAX {
        @Override
        public CentralityResult apply(CentralityResultWithStatistics scores) {
            double norm = scores.computeMax();
            return new NormalizedCentralityResult(scores, (score) -> score / norm);
        }
    },
    L1NORM {
        @Override
        public CentralityResult apply(CentralityResultWithStatistics scores) {
            double norm = scores.computeL1Norm();
            return new NormalizedCentralityResult(scores, (score) -> score / norm);
        }
    },
    L2NORM {
        @Override
        public CentralityResult apply(CentralityResultWithStatistics scores) {
            double norm = scores.computeL2Norm();
            return new NormalizedCentralityResult(scores, (score) -> score / norm);
        }
    };

    public abstract CentralityResult apply(CentralityResultWithStatistics scores);
}
