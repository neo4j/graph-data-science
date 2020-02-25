/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.core.utils;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.core.concurrency.ConcurrencyMonitor;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.graphalgo.config.ConcurrencyValidation.CONCURRENCY_LIMITATION;

class ForkJoinPoolsTest {

    @Test
    void forkJoinPoolShouldBeLimited() {
        ConcurrencyMonitor.instance().setLimited();
        assertThat(ForkJoinPools.createPool(128).getParallelism(), equalTo(CONCURRENCY_LIMITATION));
    }

    @Test
    void forkJoinPoolShouldBeUnLimited() {
        ConcurrencyMonitor.instance().setUnlimited();
        assertThat(ForkJoinPools.createPool(128).getParallelism(), equalTo(128));
    }
}