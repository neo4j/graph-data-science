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
package org.neo4j.gds.community.validation;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.NodeLabel;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TriangleCountGraphStoreValidationTest {

    @Test
    void shouldNotComplainForExistingLabels(){
        var validation = TriangleCountGraphStoreValidation.create(List.of("A"));
        assertThatNoException().isThrownBy(() -> validation.validateLabelsExist(List.of(NodeLabel.of("A"),NodeLabel.of("B"))));
    }

    @Test
    void shouldComplainForMissingLabels(){
        var validation = TriangleCountGraphStoreValidation.create(List.of("C"));
        assertThatThrownBy(()-> validation.validateLabelsExist(List.of(NodeLabel.of("A"),NodeLabel.of("B"))))
            .hasMessageContaining("TriangleCount requires the provided 'labelFilter' node label 'C' to be present in the graph");
    }

}
