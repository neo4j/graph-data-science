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
package org.neo4j.gds.centrality;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.properties.nodes.NodePropertyRecord;
import org.neo4j.gds.api.properties.nodes.NodePropertyValuesAdapter;
import org.neo4j.gds.applications.algorithms.machinery.MutateNodePropertyService;
import org.neo4j.gds.applications.algorithms.machinery.MutateStep;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.beta.pregel.PregelResult;

import java.util.Collection;
import java.util.List;

public class HitsMutateStep implements MutateStep<PregelResult, NodePropertiesWritten> {
    private final MutateNodePropertyService mutateNodePropertyService;
    private final Collection<String> nodeLabels;
    private final String authProperty;
    private final String hubProperty;
    private final String mutateProperty;



    public HitsMutateStep(
        MutateNodePropertyService mutateNodePropertyService,
        String authProperty,
        String hubProperty,
        String mutateProperty,
        Collection<String> nodeLabels
    ) {
        this.mutateNodePropertyService = mutateNodePropertyService;
        this.nodeLabels = nodeLabels;
        this.mutateProperty = mutateProperty;
        this.authProperty = authProperty;
        this.hubProperty = hubProperty;
    }

    @Override
    public NodePropertiesWritten execute(
        Graph graph,
        GraphStore graphStore,
        PregelResult result
    ) {
        var authValues = NodePropertyValuesAdapter.adapt(result.nodeValues().doubleProperties(authProperty));
        var hubValues = NodePropertyValuesAdapter.adapt(result.nodeValues().doubleProperties(hubProperty));
        var authProperty = property(this.authProperty);
        var hubProperty = property(this.hubProperty);
        
        var authRecord = NodePropertyRecord.of(authProperty, authValues);
        var hubRecord = NodePropertyRecord.of(hubProperty, hubValues);

        return mutateNodePropertyService.mutateNodeProperties(
            graph,
            graphStore,
            List.of(authRecord,hubRecord),
            nodeLabels
        );

    }
    private String property(String property){
        return property.concat(mutateProperty);
    }

}
