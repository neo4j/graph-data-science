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
package org.neo4j.gds.core;

import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.utils.CheckedRunnable;
import org.neo4j.gds.utils.GdsFeatureToggles;

import java.util.stream.Stream;

public interface TestMethodRunner {
    <E extends Exception> void run(CheckedRunnable<E> code) throws E;

    @TestOnly
    static Stream<TestMethodRunner> adjacencyCompressions() {
        return Stream.of(
            TestMethodRunner::runCompressedUnordered,
            TestMethodRunner::runCompressedOrdered,
            TestMethodRunner::runUncompressedUnordered,
            TestMethodRunner::runUncompressedOrdered,
            TestMethodRunner::runPackedUnordered,
            TestMethodRunner::runPackedOrdered
        );
    }

    @TestOnly
    static <E extends Exception> void runCompressedUnordered(CheckedRunnable<E> code) throws E {
        GdsFeatureToggles.USE_UNCOMPRESSED_ADJACENCY_LIST.disableAndRun(() ->
            GdsFeatureToggles.USE_REORDERED_ADJACENCY_LIST.disableAndRun(code));
    }

    @TestOnly
    static <E extends Exception> void runCompressedOrdered(CheckedRunnable<E> code) throws E {
        GdsFeatureToggles.USE_UNCOMPRESSED_ADJACENCY_LIST.disableAndRun(() ->
            GdsFeatureToggles.USE_REORDERED_ADJACENCY_LIST.enableAndRun(code));
    }

    @TestOnly
    static <E extends Exception> void runUncompressedUnordered(CheckedRunnable<E> code) throws E {
        GdsFeatureToggles.USE_UNCOMPRESSED_ADJACENCY_LIST.enableAndRun(() ->
            GdsFeatureToggles.USE_REORDERED_ADJACENCY_LIST.disableAndRun(code));
    }

    @TestOnly
    static <E extends Exception> void runUncompressedOrdered(CheckedRunnable<E> code) throws E {
        GdsFeatureToggles.USE_UNCOMPRESSED_ADJACENCY_LIST.enableAndRun(() ->
            GdsFeatureToggles.USE_REORDERED_ADJACENCY_LIST.enableAndRun(code));
    }

    @TestOnly
    static <E extends Exception> void runPackedUnordered(CheckedRunnable<E> code) throws E {
        GdsFeatureToggles.USE_PACKED_ADJACENCY_LIST.enableAndRun(() ->
            GdsFeatureToggles.USE_REORDERED_ADJACENCY_LIST.disableAndRun(code));
    }

    @TestOnly
    static <E extends Exception> void runPackedOrdered(CheckedRunnable<E> code) throws E {
        GdsFeatureToggles.USE_PACKED_ADJACENCY_LIST.enableAndRun(() ->
            GdsFeatureToggles.USE_REORDERED_ADJACENCY_LIST.enableAndRun(code));
    }
}
