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
package org.neo4j.gds.ml.linkmodels.pipeline;

import org.neo4j.gds.ml.linkmodels.pipeline.procedureutils.ProcedureReflection;
import org.neo4j.gds.BaseProc;
import org.neo4j.gds.ElementIdentifier;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class NodePropertyStep {
    public final String taskName;
    public final Map<String, Object> config;

    NodePropertyStep(String taskName, Map<String, Object> config) {
        this.taskName = taskName;
        this.config = config;
    }

    public void execute(BaseProc caller, String graphName, Collection<NodeLabel> nodeLabels, Collection<RelationshipType> relTypes) {
        var configCopy = new HashMap<>(config);
        var nodeLabelStrings = nodeLabels.stream().map(ElementIdentifier::name).collect(Collectors.toList());
        var relTypeStrings = relTypes.stream().map(ElementIdentifier::name).collect(Collectors.toList());
        configCopy.put("nodeLabels", nodeLabelStrings);
        configCopy.put("relationshipTypes", relTypeStrings);

        execute(caller, graphName, configCopy);
    }

    public void execute(BaseProc caller, String graphName) {
        execute(caller, graphName, config);
    }

    private void execute(BaseProc caller, String graphName, Map<String, Object> config) {
        ProcedureReflection.INSTANCE.invokeProc(caller, graphName, taskName, config);
    }
}
