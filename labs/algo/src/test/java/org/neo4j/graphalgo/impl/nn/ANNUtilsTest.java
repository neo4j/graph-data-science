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

package org.neo4j.graphalgo.impl.nn;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ANNUtilsTest {

    @Test
    public void sampleNeighbors() {
        Assertions.assertEquals(3, ANNUtils.sampleNeighbors(new long[] {1L, 3L, 5L, 7L}, 3, new Random()).length);
        Assertions.assertEquals(2, ANNUtils.sampleNeighbors(new long[] {1L, 3L}, 3, new Random()).length);
    }

    @Test
    public void kLessThanInputs() {
        Set<Integer> integers = ANNUtils.selectRandomNeighbors(3, 5, 1, new Random());
        assertThat(integers, Matchers.not(Matchers.hasItem(1)));
        assertThat(integers, Matchers.hasSize(3));
    }

    @Test
    public void kGreaterThanInputs() {
        Set<Integer> integers = ANNUtils.selectRandomNeighbors(7, 5, 1, new Random());
        assertThat(integers, Matchers.not(Matchers.hasItem(1)));
        assertThat(integers, Matchers.hasSize(4));
    }

    @Test
    public void kEqualToInputs() {
        Set<Integer> integers = ANNUtils.selectRandomNeighbors(5, 5, 3, new Random());
        assertThat(integers, Matchers.not(Matchers.hasItem(3)));
        assertThat(integers, Matchers.hasSize(4));
    }

}
