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

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

class CompoundGraphStoreValidationsTest {

    @Test
    void shouldWorkWithMultipleValidations(){
        var goodValidation1 = mock(GraphStoreValidation.class);
        var badValidation = mock(GraphStoreValidation.class);
        var goodValidation2 = mock(GraphStoreValidation.class);

        doThrow(new RuntimeException("OOPS")).when(badValidation).validateAlgorithmRequirements(any(GraphStore.class),anySet(),anySet());
        var validations = new CompoundGraphStoreValidationsBuilder()
            .withGraphStoreValidation(goodValidation1)
            .withGraphStoreValidation(badValidation)
            .withGraphStoreValidation(goodValidation2)
            .build();

        assertThatThrownBy(()->validations.validateAlgorithmRequirements(mock(GraphStore.class), Set.of(), Set.of()))
            .hasMessageContaining("OOPS");

    }

}
