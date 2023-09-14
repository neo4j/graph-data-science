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
package org.neo4j.gds.ml.kge;

import com.carrotsearch.hppc.DoubleArrayList;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.ml.core.tensor.Vector;

import static java.util.Arrays.stream;

public class EuclideanDistanceLinkScorer implements LinkScorer {

    NodePropertyValues embeddings;

    Vector relationshipTypeEmbedding;

    long currentSourceNode;

    Vector currentCandidateTarget;

    EuclideanDistanceLinkScorer(NodePropertyValues embeddings, DoubleArrayList relationshipTypeEmbedding) {
        this.embeddings = embeddings;
        this.relationshipTypeEmbedding = new Vector(relationshipTypeEmbedding.toArray());
    }

    @Override
    public void init(long sourceNode) {
        this.currentSourceNode = sourceNode;
        this.currentCandidateTarget = new Vector(embeddings.doubleArrayValue(currentSourceNode))
            .add(relationshipTypeEmbedding);
    }

    @Override
    public double computeScore(long targetNode) {
        Vector translation = currentCandidateTarget.scalarMultiply(-1)
            .add(new Vector(embeddings.doubleArrayValue(targetNode)));

        return Math.sqrt(
            stream(translation.data())
                .map(value -> value * value)
                .sum()
        );

    }

    @Override
    public void close() throws Exception { }

}