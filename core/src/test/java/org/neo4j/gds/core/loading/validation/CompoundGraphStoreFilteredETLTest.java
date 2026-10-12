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
package org.neo4j.gds.core.loading.validation;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.GraphStore;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

class CompoundGraphStoreFilteredETLTest {
    @Test
    void shouldWorkWithMultipleSteps() {
        var goodETL1 = mock(GraphStoreFilteredETL.class);
        var badETL = mock(GraphStoreFilteredETL.class);
        var goodETL2 = mock(GraphStoreFilteredETL.class);

        doThrow(new RuntimeException("OOPS")).when(badETL).onGraphStoreFiltered(
            any(GraphStore.class),
            anySet(),
            anySet(),
            any(Optional.class)
        );
        var compoundETL = new CompoundGraphStoreFilteredETL(
            List.of(
                goodETL1,
                badETL,
                goodETL2
            )
        );

        assertThatThrownBy(
            () -> compoundETL.onGraphStoreFiltered(mock(GraphStore.class), Set.of(), Set.of(), Optional.empty())
        ).hasMessageContaining("OOPS");
    }
}
