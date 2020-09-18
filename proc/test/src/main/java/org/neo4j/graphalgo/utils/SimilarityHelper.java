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
package org.neo4j.graphalgo.utils;

import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.utility.Iterate;
import org.neo4j.graphalgo.nodesim.SimilarityResult;

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class SimilarityHelper {

    private SimilarityHelper() {}

    public static void assertSimilarityStreamsAreEqual(Stream<SimilarityResult> result1, Stream<SimilarityResult> result2) {
        Collection<Pair<SimilarityResult, SimilarityResult>> comparableResults = Iterate.zip(
            result1.collect(Collectors.toList()),
            result2.collect(Collectors.toList())
        );
        for (Pair<SimilarityResult, SimilarityResult> pair : comparableResults) {
            SimilarityResult left = pair.getOne();
            SimilarityResult right = pair.getTwo();
            assertEquals(left, right);
        }
    }
}
