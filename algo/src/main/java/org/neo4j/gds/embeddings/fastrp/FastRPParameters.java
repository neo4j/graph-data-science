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

import org.neo4j.gds.annotation.Parameters;

import java.util.List;
import java.util.Optional;


@Parameters
public final class FastRPParameters {

    public static FastRPParameters create(
        List<String> featureProperties,
        List<Number> iterationWeights,
        int embeddingDimension,
        int propertyDimension,
        Optional<String> relationshipWeightProperty,
        float normalizationStrength,
        Number nodeSelfInfluence
    ) {
        return new FastRPParameters(
            featureProperties,
            iterationWeights,
            embeddingDimension,
            propertyDimension,
            relationshipWeightProperty,
            normalizationStrength,
            nodeSelfInfluence
        );
    }

    private final List<String> featureProperties;
    private final List<Number> iterationWeights;
    private final int embeddingDimension;
    private final int propertyDimension;
    private final Optional<String> relationshipWeightProperty;
    private final float normalizationStrength;
    private final Number nodeSelfInfluence;

    private FastRPParameters(
        List<String> featureProperties,
        List<Number> iterationWeights,
        int embeddingDimension,
        int propertyDimension,
        Optional<String> relationshipWeightProperty,
        float normalizationStrength,
        Number nodeSelfInfluence
    ) {
        this.featureProperties = featureProperties;
        this.iterationWeights = iterationWeights;
        this.embeddingDimension = embeddingDimension;
        this.propertyDimension = propertyDimension;
        this.relationshipWeightProperty = relationshipWeightProperty;
        this.normalizationStrength = normalizationStrength;
        this.nodeSelfInfluence = nodeSelfInfluence;
    }

    public List<String> featureProperties() {
        return featureProperties;
    }

    List<Number> iterationWeights() {
        return iterationWeights;
    }

    int embeddingDimension() {
        return embeddingDimension;
    }

    int propertyDimension() {
        return propertyDimension;
    }

    Optional<String> relationshipWeightProperty() {
        return relationshipWeightProperty;
    }

    float normalizationStrength() {
        return normalizationStrength;
    }

    Number nodeSelfInfluence() {
        return nodeSelfInfluence;
    }
}
