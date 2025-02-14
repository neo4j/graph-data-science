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
package org.neo4j.gds.hdbscan;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeLongArray;

import static org.assertj.core.api.Assertions.assertThat;

class StabilityStepTest {

    @Test
    void clusterStability() {
        var nodeCount = 4;
        var root = 4;

        var parent = HugeLongArray.of(5, 5, 6, 6, 0, 4, 4);
        var lambda = HugeDoubleArray.of(10, 10, 11, 11, 0, 12, 12);
        var size = HugeLongArray.of(4, 2, 2);
        var maximumClusterId = 6;

        var condensedTree = new CondensedTree(root, parent, lambda, size, maximumClusterId, nodeCount);
        var stabilityStep = new StabilityStep();

        var stabilities = stabilityStep.computeStabilities(condensedTree, nodeCount);


        assertThat(stabilities.toArray()).containsExactly(
            // stability of 4
            (1 / 12. - 0) + (1 / 12. - 0) + (1 / 12. - 0) + (1 / 12. - 0),
            // stability of 5
            (1 / 10. - 1 / 12.) + (1 / 10. - 1 / 12.),
            // stability of 6
            (1 / 11. - 1 / 12.) + (1 / 11. - 1 / 12.)
        );
    }

    @Test
    void clusterStabilityBiggerTest() {
        var parent = HugeLongArray.of(8, 8, 10, 10, 11, 11, 11, 0, 7, 7, 9, 9, 0);
        var lambda = HugeDoubleArray.of(11.0, 11.0, 9.0, 9.0, 8.0, 7.0, 7.0, 0.0, 12.0, 12.0, 10.0, 10.0, 0.0);
        var size = HugeLongArray.of(7, 2, 5, 2, 3, 0, 0);
        var maximumClusterId = 11;
        var nodeCount = 7;
        var root = 7;

        var condensedTree = new CondensedTree(root, parent, lambda, size, maximumClusterId, nodeCount);

        var stabilityStep = new StabilityStep();

        var stabilities = stabilityStep.computeStabilities(condensedTree, nodeCount);

        assertThat(stabilities.toArray()).containsExactly(
            new double[] {
                // stability of 7
                7 * 1. / 12,
                // stability of 8
                2 * (1. / 11 - 1. / 12),
                // stability of 9
                5 * (1. / 10 - 1. / 12),
                // stability of 10
                2 * (1. / 9 - 1. / 10),
                // stability of 11
                (1. / 8 - 1. / 10) + 2 * (1. / 7 - 1. / 10),
                0.0
            }, Offset.offset(1e-10));
    }
}
