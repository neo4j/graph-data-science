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
package org.neo4j.gds.similarity.nodesim;

import org.neo4j.gds.similarity.HugeSimilarityGraph;
import org.neo4j.gds.similarity.SimilarityGraph;
import org.neo4j.gds.similarity.SimilarityResult;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

public record NodeSimilarityResult(
    Optional<Stream<SimilarityResult>> maybeStreamResult,
    Optional<SimilarityGraph> maybeGraphResult,
    long comparedNodes
) {

    public Stream<SimilarityResult> streamResult() {
        return maybeStreamResult().orElseThrow();
    }

    public SimilarityGraph graphResult() {
        return maybeGraphResult().orElseThrow();
    }

   public static final NodeSimilarityResult EMPTY = new NodeSimilarityResult(
       Optional.of(Stream.empty()),
       Optional.of(new HugeSimilarityGraph(null, Map.of())),
       0
   );
}
