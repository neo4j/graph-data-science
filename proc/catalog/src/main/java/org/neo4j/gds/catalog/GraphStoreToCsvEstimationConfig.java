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
package org.neo4j.gds.catalog;

import org.immutables.value.Value;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.io.file.GraphStoreToFileExporterConfig;

@ValueClass
@Configuration
@SuppressWarnings("immutables:subtype")
public interface GraphStoreToCsvEstimationConfig extends GraphStoreToFileExporterConfig {

    @Value.Default
    @Configuration.DoubleRange(min = 0.0, max = 1.0)
    default double samplingFactor() {
        return 0.001;
    }

    static GraphStoreToCsvEstimationConfig of(String username, CypherMapWrapper config) {
        return new GraphStoreToCsvEstimationConfigImpl(username, config);
    }
}
