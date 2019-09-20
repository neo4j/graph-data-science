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
package org.neo4j.graphalgo.similarity;

import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.impl.results.SimilarityResult;
import org.neo4j.graphalgo.impl.similarity.SimilarityComputer;
import org.neo4j.graphalgo.impl.similarity.WeightedInput;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public class EuclideanAlgorithm extends WeightedAlgorithm {
    public EuclideanAlgorithm(GraphDatabaseAPI api, ProcedureConfiguration configuration) {
        super(api, configuration);
    }

    @Override
    public double similarityCutoff() {
        double similarityCutoff = getSimilarityCutoff(configuration);
        // as we don't compute the sqrt until the end
        if (similarityCutoff > 0d) similarityCutoff *= similarityCutoff;
        return similarityCutoff;
    }

    protected static Double getSimilarityCutoff(ProcedureConfiguration configuration) {
        return configuration.getNumber("similarityCutoff", -1D).doubleValue();
    }

    @Override
    public SimilarityComputer<WeightedInput> similarityComputer(final Double skipValue) {
        return skipValue == null ?
                (decoder, s, t, cutoff) -> s.sumSquareDelta(decoder, cutoff, t, false) :
                (decoder, s, t, cutoff) -> s.sumSquareDeltaSkip(decoder, cutoff, t, skipValue, false);
    }

    @Override
    public SimilarityResult postProcess(final SimilarityResult result) {
        return result;
    }

    @Override
    public int topK() {
        return -configuration.getNumber("topK", 3).intValue();
    }

}
