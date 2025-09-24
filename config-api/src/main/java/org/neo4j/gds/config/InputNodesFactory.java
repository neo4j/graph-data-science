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
package org.neo4j.gds.config;

import org.neo4j.gds.utils.StringFormatting;

import java.util.ArrayList;
import java.util.List;

import static org.neo4j.gds.config.ConfigNodesValidations.nodesNotNegative;

public final class InputNodesFactory {

    private InputNodesFactory() {}

    public static InputNodes parse(Object object, String parameterName){
        if ( object instanceof InputNodes) {
            return (InputNodes) object;
        }

        else if (object instanceof List<?> list && !list.isEmpty() && list.get(0) instanceof List){
            var mapsSourceNodes = new MapInputNodes(NodeIdParser.parseToMapOfNodeIdsWithProperties(object, parameterName));
            nodesNotNegative(mapsSourceNodes.inputNodes(), parameterName);
            return mapsSourceNodes;
        }
        var listSourceNodes = new ListInputNodes(NodeIdParser.parseToListOfNodeIds(object, parameterName));
        nodesNotNegative(listSourceNodes.inputNodes(), parameterName);
        return listSourceNodes;
    }

    public static InputNodes parseAsList(Object object, String parameterName) {
        if ( object instanceof InputNodes) {
            return (InputNodes) object;
        }
        var listSourceNodes = new ListInputNodes(NodeIdParser.parseToListOfNodeIds(object, parameterName));
        nodesNotNegative(listSourceNodes.inputNodes(), parameterName);
        return listSourceNodes;
    }

    public static List toMapOutput(InputNodes inputNodes, String parameterName) {
        if ( inputNodes instanceof ListInputNodes) {
            return new ArrayList<>(inputNodes.inputNodes());
        } else if ( inputNodes instanceof MapInputNodes) {
            return ((MapInputNodes) inputNodes).map()
                .entrySet()
                .stream()
                .map(entry -> List.of(entry.getKey(), entry.getValue()))
                .toList();
        }else{
            throw new RuntimeException(StringFormatting.formatWithLocale("Not valid %s",parameterName));
        }
    }
}
