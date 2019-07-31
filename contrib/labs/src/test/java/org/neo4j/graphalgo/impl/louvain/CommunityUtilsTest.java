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
package org.neo4j.graphalgo.impl.louvain;

import org.junit.Test;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.impl.utils.CommunityUtils;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class CommunityUtilsTest {
    @Test
    public void differentNumbers() {
        int[] communities = {10, 3, 4, 7, 6, 7, 10};
        assertEquals(5, CommunityUtils.normalize(communities));
        assertArrayEquals(new int[]{0, 1, 2, 3, 4, 3, 0}, communities);
    }

    @Test
    public void allTheSame() {
        int[] communities = {10, 10, 10, 10};
        assertEquals(1, CommunityUtils.normalize(communities));
        assertArrayEquals(new int[]{0, 0, 0, 0}, communities);
    }

    @Test
    public void allDifferent() {
        int[] communities = {1, 2, 3, 4, 7, 5};
        assertEquals(6, CommunityUtils.normalize(communities));
        assertArrayEquals(new int[]{0, 1, 2, 3, 4, 5}, communities);
    }

    @Test
    public void hugeDifferentNumbers() {
        HugeLongArray communities = HugeLongArray.of(10, 3, 4, 7, 6, 7, 10);
        assertEquals(5, CommunityUtils.normalize(communities));
        assertArrayEquals(new long[]{0, 1, 2, 3, 4, 3, 0}, communities.toArray());
    }

    @Test
    public void hugeAllTheSame() {
        HugeLongArray communities = HugeLongArray.of(10, 10, 10, 10);
        assertEquals(1, CommunityUtils.normalize(communities));
        assertArrayEquals(new long[]{0, 0, 0, 0}, communities.toArray());
    }

    @Test
    public void hugeAllDifferent() {
        HugeLongArray communities = HugeLongArray.of(1, 2, 3, 4, 7, 5);
        assertEquals(6, CommunityUtils.normalize(communities));
        assertArrayEquals(new long[]{0, 1, 2, 3, 4, 5}, communities.toArray());
    }
}
