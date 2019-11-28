/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo;

import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.ProcedureConstants;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

// TODO: this class should eventually disappear
public abstract class BaseAlgoProcLegacyConfig<A extends Algorithm<A>> extends BaseAlgoProc<A, ProcedureConfiguration> {

    protected double getDefaultWeightProperty(ProcedureConfiguration config) {
        return ProcedureConstants.DEFAULT_VALUE_DEFAULT;
    }

    protected ProcedureConfiguration newConfig(String label, String relationship, Map<String, Object> config) {
        config.put("label", label);
        config.put("relationship", relationship);
        return newConfig(config);
    }

    protected ProcedureConfiguration newConfig(Map<String, Object> config) {
        return newConfig(Optional.empty(), CypherMapWrapper.create(config));
    }

    protected ProcedureConfiguration newConfig(Optional<String> graphName, Map<String, Object> config) {
        return newConfig(graphName, CypherMapWrapper.create(config));
    }

    @Override
    protected ProcedureConfiguration newConfig(Optional<String> graphName, CypherMapWrapper config) {
        ProcedureConfiguration configuration = (config != null)
            ? ProcedureConfiguration.create(config, getUsername())
            : ProcedureConfiguration.create(getUsername());

        String label = config.getString("label", "");
        if (label != null && !label.isEmpty()) {
            configuration = configuration.setNodeLabelOrQuery(label);
        }
        String relationship = config.getString("relationship", "");
        if (relationship != null && !relationship.isEmpty()) {
            configuration = configuration.setRelationshipTypeOrQuery(relationship);
        }

        configuration = configuration.setAlgoSpecificDefaultWeight(getDefaultWeightProperty(configuration));

        Set<String> returnItems = callContext.outputFields().collect(Collectors.toSet());
        return configuration
            .setComputeCommunityCount(OutputFieldParser.computeCommunityCount(returnItems))
            .setComputeHistogram(OutputFieldParser.computeHistogram(returnItems));
    }
}
