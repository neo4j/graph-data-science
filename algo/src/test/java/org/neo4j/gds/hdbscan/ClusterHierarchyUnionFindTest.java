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

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
class ClusterHierarchyUnionFindTest {

    @Test
    void union(SoftAssertions assertions) {
        var u = new ClusterHierarchyUnionFind(5L);

        assertions.assertThat(u.union(3L, 4L)).isEqualTo(5L);
        assertions.assertThat(u.union(1L, 2L)).isEqualTo(6L);
        assertions.assertThat(u.union(0L, 2L)).isEqualTo(7L);
    }

    @Test
    void find(SoftAssertions assertions) {
        var u = new ClusterHierarchyUnionFind(5L);

        assertions.assertThat(u.union(3L, 4L)).isEqualTo(5L);


        assertions.assertThat(u.find(3L)).isEqualTo(5L);
        assertions.assertThat(u.find(4L)).isEqualTo(5L);

        assertions.assertThat(u.union(3L, 2L)).isEqualTo(6L);
        assertions.assertThat(u.find(3L)).isEqualTo(6L);
        assertions.assertThat(u.find(4L)).isEqualTo(5L);

        assertions.assertThat(u.find(7L)).isEqualTo(7L);
    }

}
