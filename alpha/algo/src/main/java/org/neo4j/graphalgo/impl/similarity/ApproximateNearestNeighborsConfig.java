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
package org.neo4j.graphalgo.impl.similarity;

import org.immutables.value.Value;
import org.neo4j.graphalgo.annotation.Configuration;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.newapi.IterationsConfig;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@ValueClass
@Configuration("ApproximateNearestNeighborsConfigImpl")
public interface ApproximateNearestNeighborsConfig extends SimilarityConfig, IterationsConfig {

    @Configuration.ConvertWith("similarityAlgorithm")
    SimilarityAlgorithm algorithm();

    @Override
    @Configuration.Ignore
    default String graph() {
        return "dense";
    }

    @Override
    @Value.Default
    default List<Map<String,Object>> data() {
        return Collections.emptyList();
    }

    @Override
    @Value.Default
    default int maxIterations() {
        return 10;
    }

    @Value.Default
    default double precision() {
        return 0.001;
    }

    @Value.Default
    default double p() {
        return 0.5;
    }

    @Value.Default
    default long randomSeed() {
        return 1;
    }

    @Value.Default
    default boolean sampling() {
        return true;
    }

    static SimilarityAlgorithm similarityAlgorithm(String input) {
        return SimilarityAlgorithm.valueOf(input);
    }

    enum SimilarityAlgorithm {
        jaccard, cosine, pearson, euclidean
    }
}
