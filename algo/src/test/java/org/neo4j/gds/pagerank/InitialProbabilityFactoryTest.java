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
import org.neo4j.gds.InputNodes;
import org.neo4j.gds.config.InputNodesFactory;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class InitialProbabilityFactoryTest {

    @Test
    void testInitialProbabilityEmpty() {
        var alpha = 0.15;
        var sourceNodes = InputNodes.EMPTY_INPUT_NODES;
        InitialProbabilityProvider initialProbabilityProvider = InitialProbabilityFactory.create((x) -> (2*x), alpha, sourceNodes);
        assertThat(initialProbabilityProvider).isInstanceOf(GlobalRestartProbability.class);
    }

    @Test
    void testInitialProbabilityList() {
        var alpha = 0.15;
        var sourceNodesList = InputNodesFactory.parse(List.of(0L,2L,10L),"FOO");
        InitialProbabilityProvider initialProbabilityProvider = InitialProbabilityFactory.create((x) -> (2*x), alpha, sourceNodesList);
        assertThat(initialProbabilityProvider).isInstanceOf(SourceBasedRestartProbabilityList.class);
    }

    @Test
    void testInitialProbabilityListOfLists() {
        var alpha = 0.15;
        var sourceNodesMap = InputNodesFactory.parse(List.of(List.of(0L, 1D), List.of(5L, 0.1D)),"FOO");
        InitialProbabilityProvider initialProbabilityProvider = InitialProbabilityFactory.create((x) -> (2*x), alpha, sourceNodesMap);
        assertThat(initialProbabilityProvider).isInstanceOf(SourceBasedRestartProbability.class);
    }

}
