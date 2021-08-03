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
package org.neo4j.gds.utils;

import org.neo4j.gds.similarity.SimilarityResult;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public final class SimilarityHelper {

    private SimilarityHelper() {}

    public static void assertSimilarityStreamsAreEqual(Stream<SimilarityResult> result1, Stream<SimilarityResult> result2) {
        Set<SimilarityResult> set1 = result1.collect(Collectors.toSet());
        Set<SimilarityResult> set2 = result2.collect(Collectors.toSet());
        assertThat(set1).containsExactlyInAnyOrderElementsOf(set2);
    }
}
