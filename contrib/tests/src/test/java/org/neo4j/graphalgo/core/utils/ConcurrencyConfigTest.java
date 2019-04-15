/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public final class ConcurrencyConfigTest {

    @Test
    public void limitConcurrencyOnCommunityEdition() {
        ConcurrencyConfig config = new ConcurrencyConfig(/* cpus */ 42, /* isOnEnterprise */ false);
        assertEquals(4, config.defaultConcurrency);
        assertEquals(4, config.maxConcurrency);
    }

    @Test
    public void allowLowerThanMaxSettingsOnCommunityEdition() {
        ConcurrencyConfig config = new ConcurrencyConfig(/* cpus */ 2, /* isOnEnterprise */ false);
        assertEquals(4, config.maxConcurrency);
        assertEquals(2, config.defaultConcurrency);
    }

    @Test
    public void unlimitedDefaultConcurrencyOnEnterpriseEdition() {
        ConcurrencyConfig config = new ConcurrencyConfig(/* cpus */ 42, /* isOnEnterprise */ true);
        assertEquals(42, config.defaultConcurrency);
        assertEquals(Integer.MAX_VALUE, config.maxConcurrency);
    }
}
