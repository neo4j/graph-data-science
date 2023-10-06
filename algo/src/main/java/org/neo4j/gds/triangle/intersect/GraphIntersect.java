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
package org.neo4j.gds.triangle.intersect;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.api.AdjacencyCursor;
import org.neo4j.gds.api.IntersectionConsumer;
import org.neo4j.gds.api.RelationshipIntersect;

import java.util.function.IntPredicate;

import static org.neo4j.gds.api.AdjacencyCursor.NOT_FOUND;

/**
 * An instance of this is not thread-safe; Iteration/Intersection on multiple threads will
 * throw misleading {@link NullPointerException}s.
 * Instances are however safe to use concurrently with other {@link org.neo4j.gds.api.RelationshipIterator}s.
 */

public abstract class GraphIntersect<CURSOR extends AdjacencyCursor> implements RelationshipIntersect {

    private final IntPredicate degreeFilter;

    protected GraphIntersect(long maxDegree) {
        this.degreeFilter = maxDegree < Long.MAX_VALUE
            ? (degree) -> degree <= maxDegree
            : (ignore) -> true;
    }

    @Override
    public void intersectAll(long a, IntersectionConsumer consumer) {
        // check the first node's degree
        int degreeOfa = degree(a);
        if (!degreeFilter.test(degreeOfa)) {
            return;
        }

        CURSOR origNeighborsOfa = cursorForNode(null, a, degreeOfa);

        triangles(a, degreeOfa, origNeighborsOfa, consumer);
    }

    private void triangles(
        long a,
        int degreeOfa,
        CURSOR neighborsOfa,
        IntersectionConsumer consumer
    ) {
        long b = next(neighborsOfa);
        while (b != NOT_FOUND && b < a) {
            var degreeOfb = degree(b);
            if (degreeFilter.test(degreeOfb)) {
                var helpingCursorOfb = cursorForNode(
                    null,
                    b,
                    degreeOfb
                );

                var helpingCursorOfa = cursorForNode(null, a, degreeOfa);

                triangles(
                    a,
                    b,
                    helpingCursorOfa,
                    helpingCursorOfb,
                    consumer
                ); //find all triangles involving the edge (a-b)
            }

            b = next(neighborsOfa);
        }

    }

    private void triangles(long a, long b, CURSOR neighborsOfa, CURSOR neighborsOfb, IntersectionConsumer consumer) {
        long c = next(neighborsOfb);
        long currentOfa = next(neighborsOfa);
        while (c != NOT_FOUND && currentOfa != NOT_FOUND && c < b) {
            var degreeOfc = degree(c);
            if (degreeFilter.test(degreeOfc)) {
                currentOfa = advance(neighborsOfa, currentOfa, c);
                //now print all triangles a-b-c  (taking into consideration the parallel edges of c)
                checkForAndEmitTriangle(consumer, a, b, currentOfa, c);

            }
            c = next(neighborsOfb);
        }
    }

    private void checkForAndEmitTriangle(
        IntersectionConsumer consumer,
        long a,
        long b,
        long currentOfa,
        long c
    ) {
        // if Ca = Cb there exists a triangle
        // Ca might also be NOT_FOUND, we ignore those as well

        if (currentOfa == c) {
            consumer.accept(c, b, a); // triangle is s.t that c < b < a
        }
    }

    private long advance(CURSOR adjacencyList, long start, long target) {
        long current = start;
        while (current != NOT_FOUND && current < target) {
            current = next(adjacencyList);
        }
        return current;
    }
    private long next(CURSOR adjacencyList) {

        if (!adjacencyList.hasNextVLong()) {
            return NOT_FOUND;
        }
        var value = adjacencyList.nextVLong();

        while (peek(adjacencyList) == value) {
            adjacencyList.nextVLong();
        }

        return value;
    }

    private long peek(CURSOR adjacencyList) {

        if (!adjacencyList.hasNextVLong()) {
            return NOT_FOUND;
        }

        return adjacencyList.peekVLong();
    }

    private @NotNull CURSOR copyCursor(@NotNull CURSOR source, @Nullable CURSOR destination) {
        return checkCursorInstance(source.shallowCopy(destination));
    }

    protected abstract CURSOR checkCursorInstance(AdjacencyCursor cursor);

    protected abstract CURSOR cursorForNode(@Nullable CURSOR reuse, long node, int degree);

    protected abstract int degree(long node);
}
