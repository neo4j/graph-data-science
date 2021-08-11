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
package org.neo4j.gds.ml.splitting;

import org.immutables.value.Value;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.annotation.ValueClass;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@ValueClass
@Configuration
public interface SplitRelationshipsBaseConfig {

    @Configuration.DoubleRange(min = 0.0, minInclusive = false)
    double holdoutFraction();

    @Configuration.DoubleRange(min = 0.0, minInclusive = false)
    double negativeSamplingRatio();

    @Configuration.ConvertWith("org.neo4j.gds.RelationshipType#of")
    @Configuration.ToMapValue("org.neo4j.gds.RelationshipType#toString")
    RelationshipType holdoutRelationshipType();

    @Configuration.ConvertWith("org.neo4j.gds.RelationshipType#of")
    @Configuration.ToMapValue("org.neo4j.gds.RelationshipType#toString")
    RelationshipType remainingRelationshipType();

    @Value.Default
    default List<String> nonNegativeRelationshipTypes() {
        return List.of();
    }

    @Configuration.ToMap
    @Value.Auxiliary
    @Value.Derived
    default Map<String, Object> toSplitMap() {
        return Collections.emptyMap();
    }
}
