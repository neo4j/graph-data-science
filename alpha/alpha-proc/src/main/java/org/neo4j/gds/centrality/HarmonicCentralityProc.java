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

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.NodePropertiesWriter;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.impl.closeness.HarmonicCentralityConfig;
import org.neo4j.gds.impl.harmonic.HarmonicCentrality;
import org.neo4j.gds.impl.harmonic.HarmonicCentralityAlgorithmFactory;
import org.neo4j.gds.impl.harmonic.HarmonicResult;

public abstract class HarmonicCentralityProc<PROC_RESULT> extends NodePropertiesWriter<HarmonicCentrality, HarmonicResult, HarmonicCentralityConfig, PROC_RESULT> {
    protected static final String DESCRIPTION =
        "Harmonic centrality is a way of detecting nodes that are " +
        "able to spread information very efficiently through a graph.";


    @Override
    protected HarmonicCentralityConfig newConfig(String username, CypherMapWrapper config) {
        return HarmonicCentralityConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<HarmonicCentrality, HarmonicCentralityConfig> algorithmFactory() {
        return new HarmonicCentralityAlgorithmFactory();
    }

    @SuppressWarnings("unused")
    public static final class StreamResult {
        public final long nodeId;
        public final double centrality;

        StreamResult(long nodeId, double centrality) {
            this.nodeId = nodeId;
            this.centrality = centrality;
        }
    }
}
