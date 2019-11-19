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

package org.neo4j.graphalgo.newapi;

import org.neo4j.graphalgo.NodeFilters;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.RelationshipFilters;
import org.neo4j.graphalgo.annotation.Configuration;
import org.neo4j.graphalgo.annotation.Configuration.ConvertWith;
import org.neo4j.graphalgo.annotation.Configuration.Parameter;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.ProcedureConstants;
import org.neo4j.graphalgo.core.utils.Pools;

@Configuration("GraphCreateConfigImpl")
public interface GraphCreateConfig {

    @Parameter
    String graphName();

    @Parameter
    @ConvertWith("org.neo4j.graphalgo.NodeFilters#fromObject")
    NodeFilters nodeFilter();

    @Parameter
    @ConvertWith("org.neo4j.graphalgo.RelationshipFilters#fromObject")
    RelationshipFilters relationshipFilter();

    @ConvertWith("org.neo4j.graphalgo.PropertyMappings#fromObject")
    default PropertyMappings nodeProperties() {
        return PropertyMappings.EMPTY;
    }

    @ConvertWith("org.neo4j.graphalgo.PropertyMappings#fromObject")
    default PropertyMappings relationshipProperties() {
        return PropertyMappings.EMPTY;
    }

    @Configuration.Key(ProcedureConstants.READ_CONCURRENCY_KEY)
    default int concurrency() {
        return Pools.DEFAULT_CONCURRENCY;
    }

    static GraphCreateConfig legacyFactory(String graphName) {
        return new GraphCreateConfigImpl(
            graphName,
            NodeFilters.empty(),
            RelationshipFilters.empty(),
            PropertyMappings.EMPTY,
            PropertyMappings.EMPTY,
            -1
        );
    }

    static GraphCreateConfig of(
        String graphName,
        Object nodeFilter,
        Object relationshipFilter,
        CypherMapWrapper config
    ) {
        return new GraphCreateConfigImpl(
            graphName,
            nodeFilter,
            relationshipFilter,
            config
        );
    }
}
