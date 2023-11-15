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
package org.neo4j.gds.ml.kge.scorers;

import com.carrotsearch.hppc.DoubleArrayList;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.ml.kge.ScoreFunction;

public final class LinkScorerFactory {

    public static LinkScorer create(
        ScoreFunction scoreFunction,
        NodePropertyValues embeddings,
        DoubleArrayList relationshipTypeEmbedding
    ) {
        switch (scoreFunction) {
            case TRANSE:
                if (embeddings.valueType() == ValueType.FLOAT_ARRAY) {
                    return new FloatEuclideanDistanceLinkScorer(embeddings, relationshipTypeEmbedding);
                } else if (embeddings.valueType() == ValueType.DOUBLE_ARRAY) {
                    return new DoubleEuclideanDistanceLinkScorer(embeddings, relationshipTypeEmbedding);
                } else {
                    throw new IllegalArgumentException("Unsupported embeddings value type:" + embeddings.valueType());
                }
            case DISTMULT:
                if (embeddings.valueType() == ValueType.FLOAT_ARRAY) {
                    return new FloatDistMultLinkScorer(embeddings, relationshipTypeEmbedding);
                } else if (embeddings.valueType() == ValueType.DOUBLE_ARRAY) {
                    return new DoubleDistMultLinkScorer(embeddings, relationshipTypeEmbedding);
                } else {
                    throw new IllegalArgumentException("Unsupported embeddings value type:" + embeddings.valueType());
                }
            default:
                throw new IllegalArgumentException("Unknown score function:" + scoreFunction);
        }
    }

    private LinkScorerFactory() {}
}
