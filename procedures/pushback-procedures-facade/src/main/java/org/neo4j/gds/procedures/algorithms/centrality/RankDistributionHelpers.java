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
package org.neo4j.gds.procedures.algorithms.centrality;

import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.result.CentralityDistribution;
import org.neo4j.gds.scaling.LogScaler;
import org.neo4j.gds.scaling.ScalerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.function.LongToDoubleFunction;

import static org.neo4j.gds.utils.StringFormatting.toUpperCaseWithLocale;

public class RankDistributionHelpers {

    private static final String HISTOGRAM_ERROR_KEY = "Error";

    private RankDistributionHelpers() {}

    public static CentralityDistribution compute(
        IdMap graph,
        ScalerFactory scalerFactory,
        LongToDoubleFunction centralityFunction,
        Concurrency concurrency,
        boolean shouldComputeCentralityDistribution
    ) {
        var usingLogScaler = scalerFactory.type().equals(LogScaler.TYPE);

        if (shouldComputeCentralityDistribution && usingLogScaler){
                Map<String, Object> centralitySummary = new HashMap<>();
                centralitySummary.put(
                    HISTOGRAM_ERROR_KEY,
                    "Unable to create histogram when using scaler of type " + toUpperCaseWithLocale(LogScaler.TYPE)
                );
                return  new CentralityDistribution(
                    centralitySummary,
                    0
                );
        }
        return CentralityDistributionHelpers.compute(
                graph,
                centralityFunction,
                concurrency,
                shouldComputeCentralityDistribution
        );
    }
}
