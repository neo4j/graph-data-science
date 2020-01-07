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
package org.neo4j.graphalgo.impl.similarity.modern;

import org.neo4j.graphalgo.impl.similarity.CategoricalInput;
import org.neo4j.graphalgo.impl.similarity.SimilarityComputer;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public final class ModernJaccardAlgorithm extends ModernCategoricalSimilarityAlgorithm<ModernJaccardAlgorithm> {

    public ModernJaccardAlgorithm(ModernSimilarityConfig config, GraphDatabaseAPI api) {
        super(config, api);
    }

    @Override
    SimilarityComputer<CategoricalInput> similarityComputer(
        Double skipValue,
        int[] sourceIndexIds,
        int[] targetIndexIds
    ) {
        return (decoder, s, t, cutoff) -> s.jaccard(cutoff, t, false);
    }
}
