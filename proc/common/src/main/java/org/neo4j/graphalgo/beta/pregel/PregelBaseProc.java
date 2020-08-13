/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.beta.pregel;

import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.core.write.ImmutableNodeProperty;
import org.neo4j.graphalgo.core.write.NodePropertyExporter;

import java.util.List;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

final class PregelBaseProc {

    static <ALGO extends Algorithm<ALGO, Pregel.PregelResult>, CONFIG extends PregelConfig>
    List<NodePropertyExporter.NodeProperty<?>> nodeProperties(
        AlgoBaseProc.ComputationResult<ALGO, Pregel.PregelResult, CONFIG> computationResult,
        String propertyPrefix
    ) {
        var compositeNodeValue = computationResult.result().compositeNodeValues();
        var schema = compositeNodeValue.schema();
        // TODO change this to generic prefix setting

        return schema.elements().stream().map(schemaElement -> {
            NodeProperties nodeProperties;
            if (schemaElement.propertyType() == ValueType.DOUBLE) {
                nodeProperties = compositeNodeValue.doubleProperties(schemaElement.propertyKey()).asNodeProperties();
            } else {
                nodeProperties = compositeNodeValue.longProperties(schemaElement.propertyKey()).asNodeProperties();
            }

            return ImmutableNodeProperty.of(
                formatWithLocale("%s%s", propertyPrefix, schemaElement.propertyKey()),
                nodeProperties
            );
        }).collect(Collectors.toList());
    }

    private PregelBaseProc() {}

}
