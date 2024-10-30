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
package org.neo4j.gds.applications.graphstorecatalog;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.neo4j.gds.config.GraphProjectConfig;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Just a stub to facilitate testing
 */
class StubGraphProjectConfig implements GraphProjectConfig {
    private final String username;
    private final String graphName;

    StubGraphProjectConfig(String username, String graphName) {
        this.username = username;
        this.graphName = graphName;
    }

    StubGraphProjectConfig() {
        this(UUID.randomUUID().toString(), UUID.randomUUID().toString());
    }

    @Override
    public String username() {
        return username;
    }

    @Override
    public String graphName() {
        return graphName;
    }

    @Override
    public Map<String, Object> asProcedureResultConfigurationField() {
        throw new UnsupportedOperationException("TODO");
    }

//    @Override
//    public GraphStoreFactory.Supplier graphStoreFactory() {
//        throw new UnsupportedOperationException("TODO");
//    }

    @Override
    public Optional<String> usernameOverride() {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
}
