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
package org.neo4j.gds.model.catalog;

import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.gdl.GdlFactory;

import java.util.Map;

abstract class ModelProcBaseTest extends BaseProcTest {

    static final GraphSchema GRAPH_SCHEMA =
        GdlFactory.of("(:Node {p: 1})-[:REL {r: 0}]->(:Node)").build().schema();

    static final Map<String, Map<String, Map<String, String>>> EXPECTED_SCHEMA = Map.of(
        "nodes", Map.of(
            "Node", Map.of(
                "p", "Integer (DefaultValue(-9223372036854775808), TRANSIENT)"
            )
        ),
        "relationships", Map.of(
            "REL", Map.of(
                "r", "Float (DefaultValue(NaN), TRANSIENT, Aggregation.NONE)"
            )
        ),
        "graphProperties", Map.of()
    );
}
