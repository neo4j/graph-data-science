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
package org.neo4j.gds.ml.decisiontree;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TreeNodeTest {

    @Test
    void shouldEstimateSplitNode() {
        assertThat(TreeNode.splitMemoryEstimation()).isEqualTo(40);
    }

    @Test
    void shouldEstimateClassificationLeaf() {
        assertThat(TreeNode.leafMemoryEstimation(Integer.class)).isEqualTo(56);
    }

    @Test
    void shouldEstimateRegressionLeaf() {
        assertThat(TreeNode.leafMemoryEstimation(Double.class)).isEqualTo(64);
    }
}
