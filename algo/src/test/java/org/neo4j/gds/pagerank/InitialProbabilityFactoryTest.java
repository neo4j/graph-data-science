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
package org.neo4j.gds.pagerank;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.config.SourceNodes;
import org.neo4j.gds.config.SourceNodesFactory;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class InitialProbabilityFactoryTest {
    void testInitialProbabilityEmpty() {
        var alpha = 0.15;
        var sourceNodes = SourceNodes.EMPTY_SOURCE_NODES;
        InitialProbabilityProvider initialProbabilityProvider = InitialProbabilityFactory.create((x) -> (2*x), alpha, sourceNodes);
        assertThat(initialProbabilityProvider).isInstanceOf(GlobalRestartProbability.class);
    }

    void testInitialProbabilityList() {
        var alpha = 0.15;
        var sourceNodesList = SourceNodesFactory.parse(List.of(0L,2L,10L));
        InitialProbabilityProvider initialProbabilityProvider = InitialProbabilityFactory.create((x) -> (2*x), alpha, sourceNodesList);
        assertThat(initialProbabilityProvider).isInstanceOf(SourceBasedRestartProbabilityList.class);
    }

    void testInitialProbabilityMap() {
        var alpha = 0.15;
        var sourceNodesMap = SourceNodesFactory.parse(Map.of(0L, 1D, 5L, 0.1D));
        InitialProbabilityProvider initialProbabilityProvider = InitialProbabilityFactory.create((x) -> (2*x), alpha, sourceNodesMap);
        assertThat(initialProbabilityProvider).isInstanceOf(SourceBasedRestartProbability.class);
    }

}
