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

import org.neo4j.gds.annotation.Configuration;

import java.util.Map;

@Configuration
public interface GraphSampleProcConfig extends GraphProjectConfig, GraphNameConfig {

    @Configuration.Ignore
    default Map<String, Object> asProcedureResultConfigurationField() {
        var result = originalConfig().asProcedureResultConfigurationField();
        var cleansedSampleAlgoConfig = cleansed(
            sampleAlgoConfig().toMap(),
            sampleAlgoConfig().outputFieldDenylist()
        );
        result.putAll(cleansedSampleAlgoConfig);
        return result;
    }

    @Configuration.Parameter
    GraphProjectConfig originalConfig();

    @Configuration.Parameter
    String fromGraphName();

    @Configuration.Parameter
    GraphSampleAlgoConfig sampleAlgoConfig();

//    @Configuration.Ignore
//    @Override
//    default GraphStoreFactory.Supplier graphStoreFactory() {
//        return originalConfig().graphStoreFactory();
//    }

}
