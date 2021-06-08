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
package org.neo4j.graphalgo.doc.syntax;

import org.neo4j.graphalgo.similarity.SimilarityMutateResult;
import org.neo4j.graphalgo.similarity.SimilarityResult;
import org.neo4j.graphalgo.similarity.SimilarityStatsResult;
import org.neo4j.graphalgo.similarity.SimilarityWriteResult;

import java.util.Map;

import static org.neo4j.graphalgo.doc.syntax.GenericSyntaxTreeProcessor.SyntaxMode.MUTATE;
import static org.neo4j.graphalgo.doc.syntax.GenericSyntaxTreeProcessor.SyntaxMode.STATS;
import static org.neo4j.graphalgo.doc.syntax.GenericSyntaxTreeProcessor.SyntaxMode.STREAM;
import static org.neo4j.graphalgo.doc.syntax.GenericSyntaxTreeProcessor.SyntaxMode.WRITE;

class KnnSyntaxTest extends SyntaxTestBase {

    @Override
    protected Map<GenericSyntaxTreeProcessor.SyntaxMode, Class<?>> syntaxModes() {
        return Map.of(
            STREAM, SimilarityResult.class,
            STATS, SimilarityStatsResult.class,
            MUTATE, SimilarityMutateResult.class,
            WRITE, SimilarityWriteResult.class
        );
    }

    @Override
    String adocFile() {
        return "algorithms/beta/knn.adoc";
    }
}
