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
package org.neo4j.gds.similarity.nodesim;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class NodeSimilarityBaseConfigTest {

    @Test
    void shouldNotRequestAnythingIfNotEnabled(){
        var config = NodeSimilarityStreamConfigImpl.builder().build();

        assertThat(config.actuallyRunWCC()).isFalse();
        assertThat(config.enableComponentsOptimization()).isFalse();
    }

    @Test
    void shouldRequestWccIfEnabled(){
        var config = NodeSimilarityStreamConfigImpl.builder().applyWcc(true).build();

        assertThat(config.actuallyRunWCC()).isTrue();
        assertThat(config.enableComponentsOptimization()).isTrue();
    }

    @Test
    void shouldNotRequestWccIfPropertyGiven(){
        var config = NodeSimilarityStreamConfigImpl.builder().componentProperty("foo").build();

        assertThat(config.actuallyRunWCC()).isFalse();
        assertThat(config.enableComponentsOptimization()).isTrue();
    }

    @Test
    void shouldNotRequestWccIfPropertyGivenAndWccProvided(){
        var config = NodeSimilarityStreamConfigImpl.builder().applyWcc(true).componentProperty("foo").build();

        assertThat(config.actuallyRunWCC()).isFalse();
        assertThat(config.enableComponentsOptimization()).isTrue();
    }

}
