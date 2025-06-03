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
package org.neo4j.gds.msbfs;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SourceNodeSpecFactoryTest {

    @Test
    void shouldGenerateEmpty(){
        var producer = SourceNodeSpecFactory.createProducer(null);

        var spec = producer.create(5,2);
        assertThat(spec).isInstanceOf(EmptySourceNodesSpec.class);
        assertThat(spec.nodes()).containsExactly(5,6);

    }

    @Test
    void shouldGenerateList(){
        var list = new long[]{-1,-1,-1,-1,5,6,-1,-1,-1,-1};

        var producer = SourceNodeSpecFactory.createProducer(list);

        var spec = producer.create(4,2);
        assertThat(spec).isInstanceOf(ListSourceNodesSpec.class);
        assertThat(spec.nodes()).containsExactly(5,6);

    }

}
