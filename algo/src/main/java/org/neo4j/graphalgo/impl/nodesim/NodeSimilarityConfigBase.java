/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

package org.neo4j.graphalgo.impl.nodesim;

import org.immutables.value.Value;
import org.neo4j.graphalgo.annotation.Configuration;
import org.neo4j.graphalgo.newapi.BaseAlgoConfig;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.graphdb.Direction;

public interface NodeSimilarityConfigBase extends BaseAlgoConfig {

    String TOP_K_KEY = "topK";
    int TOP_K_DEFAULT = 10;

    String TOP_N_KEY = "topN";
    int TOP_N_DEFAULT = 0;

    String BOTTOM_K_KEY = "bottomK";
    int BOTTOM_K_DEFAULT = TOP_K_DEFAULT;

    String BOTTOM_N_KEY = "bottomN";
    int BOTTOM_N_DEFAULT = TOP_N_DEFAULT;

    @Value.Default
    default double similarityCutoff() {
        return 1E-42;
    }

    @Value.Default
    default int degreeCutoff() {
        return 1;
    }

    @Value.Default
    @Configuration.Key(TOP_K_KEY)
    default int topK() {
        return TOP_K_DEFAULT;
    }

    @Value.Default
    @Configuration.Key(TOP_N_KEY)
    default int topN() {
        return TOP_N_DEFAULT;
    }

    @Value.Default
    @Configuration.Key(BOTTOM_K_KEY)
    default int bottomK() {
        return BOTTOM_K_DEFAULT;
    }

    @Value.Default
    @Configuration.Key(BOTTOM_N_KEY)
    default int bottomN() {
        return BOTTOM_N_DEFAULT;
    }

    @Configuration.Ignore
    @Value.Derived
    default int normalizedK() {
        return bottomK() != BOTTOM_K_DEFAULT
            ? -bottomK()
            : topK();
    }

    @Configuration.Ignore
    @Value.Derived
    default int normalizedN() {
        return bottomN() != BOTTOM_N_DEFAULT
            ? -bottomN()
            : topN();
    }

    @Configuration.Ignore
    @Value.Derived
    default boolean isParallel() {
        return concurrency() > 1;
    }

    @Configuration.Ignore
    @Value.Derived
    default boolean hasTopK() {
        return topK() != TOP_K_DEFAULT || bottomK() != BOTTOM_K_DEFAULT;
    }

    @Configuration.Ignore
    @Value.Derived
    default boolean hasTopN() {
        return topN() != TOP_N_DEFAULT || bottomN() != BOTTOM_N_DEFAULT;
    }

    @Configuration.Ignore
    default boolean computeToStream() {
        return false;
    }

    // TODO: Remove later
    @Configuration.ConvertWith("org.neo4j.graphalgo.Projection#parseDirection")
    @Value.Default
    default Direction direction() {
        return Direction.OUTGOING;
    }

    @Value.Check
    @Configuration.Ignore
    default void validate() {
        if (degreeCutoff() < 1) {
            throw new IllegalArgumentException("Must set degree cutoff to 1 or greater");
        }

        if (topK() != TOP_K_DEFAULT && bottomK() != BOTTOM_K_DEFAULT) {
            throw new IllegalArgumentException(String.format(
                "Invalid parameter combination: %s combined with %s",
                TOP_K_KEY,
                BOTTOM_K_KEY
            ));
        }
        if (topN() != TOP_N_DEFAULT && bottomN() != BOTTOM_N_DEFAULT) {
            throw new IllegalArgumentException(String.format(
                "Invalid parameter combination: %s combined with %s",
                TOP_N_KEY,
                BOTTOM_N_KEY
            ));
        }

        String kMessage = "Invalid value for %s: must be a positive integer";
        if (bottomK() < 1) {
            throw new IllegalArgumentException(String.format(kMessage, BOTTOM_K_KEY));
        }
        if (topK() < 1) {
            throw new IllegalArgumentException(String.format(kMessage, TOP_K_KEY));
        }
        String nMessage = "Invalid value for %s: must be a positive integer or zero";
        if (bottomN() < 0) {
            throw new IllegalArgumentException(String.format(nMessage, BOTTOM_N_KEY));
        }
        if (topN() < 0) {
            throw new IllegalArgumentException(String.format(nMessage, TOP_N_KEY));
        }
    }

    interface Builder {

        Builder username(String username);

        Builder concurrency(int concurrency);

        Builder graphName(String graphName);

        Builder implicitCreateConfig(GraphCreateConfig implicitCreateConfig);

        Builder similarityCutoff(double similarityCutoff);

        Builder degreeCutoff(int degreeCutoff);

        Builder topK(int topK);

        Builder topN(int topN);

        Builder bottomK(int bottomK);

        Builder bottomN(int bottomN);

        Builder direction(Direction direction);

        NodeSimilarityConfigBase build();
    }

}
