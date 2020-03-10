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
package org.neo4j.graphalgo.nodesim;

import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.GraphMutationTest;
import org.neo4j.graphalgo.core.CypherMapWrapper;

import java.util.Optional;

class NodeSimilarityMutateProcTest
    extends NodeSimilarityBaseProcTest<NodeSimilarityWriteConfig>
    implements GraphMutationTest<NodeSimilarityWriteConfig, NodeSimilarityResult> {

    private static final String WRITE_PROPERTY = "similarity";
    private static final String WRITE_RELATIONSHIP_TYPE = "SIMILAR_TO";

    @Override
    public Class<? extends AlgoBaseProc<?, NodeSimilarityResult, NodeSimilarityWriteConfig>> getProcedureClazz() {
        return NodeSimilarityMutateProc.class;
    }

    @Override
    public NodeSimilarityMutateConfig createConfig(CypherMapWrapper mapWrapper) {
        return NodeSimilarityMutateConfig.of(getUsername(), Optional.empty(), Optional.empty(), mapWrapper);
    }

    @Override
    public CypherMapWrapper createMinimalConfig(CypherMapWrapper mapWrapper) {
        if (!mapWrapper.containsKey("writeProperty")) {
            mapWrapper = mapWrapper.withString("writeProperty", WRITE_PROPERTY);
        }
        if (!mapWrapper.containsKey("writeRelationshipType")) {
            mapWrapper = mapWrapper.withString("writeRelationshipType", WRITE_RELATIONSHIP_TYPE);
        }
        return mapWrapper;
    }

    @Override
    public void testMutateFailsOnExistingNodeProperty() {

    }

    @Override
    public String expectedMutatedGraph() {
        return String.format(
            "  (a)" +
            ", (b)" +
            ", (c)" +
            ", (d)" +
            ", (i1)" +
            ", (i2)" +
            ", (i3)" +
            ", (i4)" +
            // LIKES
            ", (a)-[{w: 1.0d}]->(i1)" +
            ", (a)-[{w: 1.0d}]->(i2)" +
            ", (a)-[{w: 1.0d}]->(i3)" +
            ", (b)-[{w: 1.0d}]->(i1)" +
            ", (b)-[{w: 1.0d}]->(i2)" +
            ", (c)-[{w: 1.0d}]->(i3)" +
            // SIMILAR_TO
            ", (a)-[{w: %f}]->(b)" +
            ", (a)-[{w: %f}]->(c)" +
            ", (b)-[{w: %f}]->(a)" +
            ", (c)-[{w: %f}]->(a)"
            , 2 / 3.0
            , 1 / 3.0
            , 2 / 3.0
            , 1 / 3.0
        );
    }
}
