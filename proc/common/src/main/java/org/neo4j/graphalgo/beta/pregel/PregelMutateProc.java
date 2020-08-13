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

import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.MutateProc;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.core.write.ImmutableNodeProperty;
import org.neo4j.graphalgo.core.write.NodePropertyExporter;

import java.util.List;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public abstract class PregelMutateProc<
    ALGO extends Algorithm<ALGO, Pregel.PregelResult>,
    CONFIG extends PregelConfig>
    extends MutateProc<ALGO, Pregel.PregelResult, PregelMutateResult, CONFIG> {

    @Override
    protected List<NodePropertyExporter.NodeProperty<?>> nodeProperties(ComputationResult<ALGO, Pregel.PregelResult, CONFIG> computationResult) {
        var compositeNodeValue = computationResult.result().compositeNodeValues();
        var schema = compositeNodeValue.schema();
        var prefix = computationResult.config().mutateProperty();

        return schema.elements().stream().map(schemaElement -> {
            NodeProperties nodeProperties;
            if (schemaElement.propertyType() == ValueType.DOUBLE) {
                nodeProperties = compositeNodeValue.doubleProperties(schemaElement.propertyKey()).asNodeProperties();
            } else {
                nodeProperties = compositeNodeValue.longProperties(schemaElement.propertyKey()).asNodeProperties();
            }

            return ImmutableNodeProperty.of(formatWithLocale("%s%s", prefix, schemaElement.propertyKey()), nodeProperties);
        }).collect(Collectors.toList());
    }

    @Override
    protected NodeProperties getNodeProperties(ComputationResult<ALGO, Pregel.PregelResult, CONFIG> computationResult) {
        throw new UnsupportedOperationException();
    }
}
