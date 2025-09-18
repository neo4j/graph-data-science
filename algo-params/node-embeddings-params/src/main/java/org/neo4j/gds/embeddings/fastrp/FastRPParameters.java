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
package org.neo4j.gds.embeddings.fastrp;

import org.neo4j.gds.AlgorithmParameters;
import org.neo4j.gds.annotation.Parameters;
import org.neo4j.gds.core.concurrency.Concurrency;

import java.util.List;
import java.util.Optional;

@Parameters
public record FastRPParameters(
    List<String> featureProperties,
    List<Number> iterationWeights,
    int embeddingDimension,
    double propertyRatio,
    Optional<String> relationshipWeightProperty,
    float normalizationStrength,
    Number nodeSelfInfluence,
    Concurrency concurrency,
    Optional<Long> randomSeed
)  implements AlgorithmParameters {
    public int propertyDimension() {
        return (int) (embeddingDimension() * propertyRatio());
    }
}
