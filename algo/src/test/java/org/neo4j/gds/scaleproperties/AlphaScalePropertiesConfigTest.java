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
package org.neo4j.gds.scaleproperties;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatNoException;

class AlphaScalePropertiesConfigTest {

    @Test
    void streamShouldNotFailOnL1L2Scalers() {
        assertThatNoException().isThrownBy(() -> new AlphaScalePropertiesStreamConfigImpl(
            CypherMapWrapper.create(
                Map.of(
                    "scaler", "l1norm",
                    "nodeProperties", List.of("a")
                )
            )
        ));
    }

    @Test
    void mutateShouldNotFailOnL1L2Scalers() {
        assertThatNoException().isThrownBy(() -> new AlphaScalePropertiesMutateConfigImpl(
            CypherMapWrapper.create(
                Map.of(
                    "mutateProperty", "foo",
                    "scaler", "l1norm",
                    "nodeProperties", List.of("a")
                )
            )
        ));
    }


}
