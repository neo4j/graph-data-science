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

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.AdjacencyCursor;
import org.neo4j.gds.api.AdjacencyCursorUtils;
import org.neo4j.gds.api.IntersectionConsumer;
import org.neo4j.gds.api.RelationshipIntersect;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.IntPredicate;

import static org.neo4j.gds.api.AdjacencyCursor.NOT_FOUND;

/**
 * An instance of this is not thread-safe; Iteration/Intersection on multiple threads will
 * throw misleading {@link NullPointerException}s.
 * Instances are however safe to use concurrently with other {@link org.neo4j.gds.api.properties.relationships.RelationshipIterator}s.
 */

public abstract class GraphIntersect<CURSOR extends AdjacencyCursor> implements RelationshipIntersect {

    private final IntPredicate degreeFilter;
    private final BiFunction<Long, NodeLabel, Boolean> hasLabel;
    private final Optional<NodeLabel> aLabel;
    private final Optional<NodeLabel> bLabel;
    private final Optional<NodeLabel> cLabel;
    private CURSOR origNeighborsOfa;
    private CURSOR helpingCursorOfa;
    private CURSOR helpingCursorOfb;


    protected GraphIntersect(
        long maxDegree,
        BiFunction<Long, NodeLabel, Boolean> hasLabel,
        Optional<NodeLabel> aLabel,
        Optional<NodeLabel> bLabel,
        Optional<NodeLabel> cLabel
    ) {
        this.degreeFilter = maxDegree < Long.MAX_VALUE
            ? (degree) -> degree <= maxDegree
            : (ignore) -> true;
        this.hasLabel = hasLabel;
        this.aLabel = aLabel;
        this.bLabel = bLabel;
        this.cLabel = cLabel;
    }

    @Override
    public void intersectAll(
        long a,
        IntersectionConsumer consumer
    ) {
        // check the first node's degree
        int degreeOfa = degree(a);
        if (!degreeFilter.test(degreeOfa)) {
            return;
        }

        origNeighborsOfa = cursorForNode(origNeighborsOfa, a, degreeOfa);

        triangles(a, degreeOfa, origNeighborsOfa, consumer);
    }

    private void triangles(
        long a,
        int degreeOfa,
        CURSOR neighborsOfa,
        IntersectionConsumer consumer
    ) {
        long b = AdjacencyCursorUtils.next(neighborsOfa);
        while (b != NOT_FOUND && (b < a)) {
            // No filters
            if ((aLabel.isEmpty() && bLabel.isEmpty() && cLabel.isEmpty())
                // One filter
                || (bLabel.isEmpty() && cLabel.isEmpty() && (hasLabel.apply(a, aLabel.get()) || hasLabel.apply(b, aLabel.get())))
                // Two filters
                || (cLabel.isEmpty() && aLabel.isPresent() && bLabel.isPresent()
                    && (hasLabel.apply(a, aLabel.get()) || hasLabel.apply(a, bLabel.get()) || hasLabel.apply(b, aLabel.get()) && hasLabel.apply(b, bLabel.get())))
                // Three filters
                || (aLabel.isPresent() && bLabel.isPresent() && cLabel.isPresent()
                    && ((hasLabel.apply(a, aLabel.get()) && hasLabel.apply(b, bLabel.get()))
                    || (hasLabel.apply(a, aLabel.get()) && hasLabel.apply(b, cLabel.get()))
                    || (hasLabel.apply(a, bLabel.get()) && hasLabel.apply(b, aLabel.get()))
                    || (hasLabel.apply(a, bLabel.get()) && hasLabel.apply(b, cLabel.get()))
                    || (hasLabel.apply(a, cLabel.get()) && hasLabel.apply(b, aLabel.get()))
                    || (hasLabel.apply(a, cLabel.get()) && hasLabel.apply(b, bLabel.get()))))) {
                var degreeOfb = degree(b);
                if (degreeFilter.test(degreeOfb)) {
                    helpingCursorOfb = cursorForNode(
                        helpingCursorOfb,
                        b,
                        degreeOfb
                    );

                    helpingCursorOfa = cursorForNode(helpingCursorOfa, a, degreeOfa);

                    triangles(
                        a,
                        b,
                        helpingCursorOfa,
                        helpingCursorOfb,
                        consumer
                    ); //find all triangles involving the edge (a-b)
                }
            }

            b = AdjacencyCursorUtils.next(neighborsOfa);
        }

    }

    private void triangles(
        long a,
        long b,
        CURSOR neighborsOfa,
        CURSOR neighborsOfb,
        IntersectionConsumer consumer
    ) {
        long c = AdjacencyCursorUtils.next(neighborsOfb);
        long currentOfa = AdjacencyCursorUtils.next(neighborsOfa);
        while (c != NOT_FOUND && currentOfa != NOT_FOUND && (c < b)) {
            // No filters
            if ((aLabel.isEmpty() && bLabel.isEmpty() && cLabel.isEmpty())
                // One filter
                || (bLabel.isEmpty() && cLabel.isEmpty() && (hasLabel.apply(a, aLabel.get()) || hasLabel.apply(b, aLabel.get()) || hasLabel.apply(c, aLabel.get())))
                // Two filters
                || (cLabel.isEmpty() && aLabel.isPresent() && bLabel.isPresent()
                    && ((hasLabel.apply(a, bLabel.get()) && (hasLabel.apply(b, aLabel.get()) || hasLabel.apply(c, aLabel.get())))
                    || (hasLabel.apply(b, bLabel.get()) && (hasLabel.apply(a, aLabel.get()) || hasLabel.apply(c, aLabel.get())))
                    || (hasLabel.apply(c, bLabel.get()) && (hasLabel.apply(a, aLabel.get()) || hasLabel.apply(b, aLabel.get())))))
                // Three filters
                || (aLabel.isPresent() && bLabel.isPresent() && cLabel.isPresent())
                    && ((hasLabel.apply(a, aLabel.get()) && hasLabel.apply(b, bLabel.get()) && hasLabel.apply(c, cLabel.get()))
                    || (hasLabel.apply(a, aLabel.get()) && hasLabel.apply(c, aLabel.get()) && hasLabel.apply(b, cLabel.get()))
                    || (hasLabel.apply(b, aLabel.get()) && hasLabel.apply(a, bLabel.get()) && hasLabel.apply(c, cLabel.get()))
                    || (hasLabel.apply(b, aLabel.get()) && hasLabel.apply(c, bLabel.get()) && hasLabel.apply(a, cLabel.get()))
                    || (hasLabel.apply(c, aLabel.get()) && hasLabel.apply(a, bLabel.get()) && hasLabel.apply(b, cLabel.get()))
                    || (hasLabel.apply(c, aLabel.get()) && hasLabel.apply(b, bLabel.get()) && hasLabel.apply(a, cLabel.get())))) {

                var degreeOfc = degree(c);
                if (degreeFilter.test(degreeOfc)) {
                    currentOfa = AdjacencyCursorUtils.advance(neighborsOfa, currentOfa, c);
                    //now print all triangles a-b-c  (taking into consideration the parallel edges of c)
                    checkForAndEmitTriangle(consumer, a, b, currentOfa, c);
                }
            }
            c = AdjacencyCursorUtils.next(neighborsOfb);
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


    protected abstract CURSOR cursorForNode(@Nullable CURSOR reuse, long node, int degree);

    protected abstract int degree(long node);
}
