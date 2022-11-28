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
package org.neo4j.gds.approxmaxkcut;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.MutateNodePropertyTest;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.impl.approxmaxkcut.ApproxMaxKCut;
import org.neo4j.gds.impl.approxmaxkcut.config.ApproxMaxKCutMutateConfig;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.TestSupport.fromGdl;

class ApproxMaxKCutMutateProcTest extends ApproxMaxKCutProcTest<ApproxMaxKCutMutateConfig> implements
    MutateNodePropertyTest<ApproxMaxKCut, ApproxMaxKCutMutateConfig, ApproxMaxKCut.CutResult> {

    @Override
    public String mutateProperty() {
        return "community";
    }

    @Override
    public ValueType mutatePropertyType() {
        return ValueType.LONG;
    }

    @Override
    public Class<? extends AlgoBaseProc<ApproxMaxKCut, ApproxMaxKCut.CutResult, ApproxMaxKCutMutateConfig, ?>> getProcedureClazz() {
        return ApproxMaxKCutMutateProc.class;
    }

    @Override
    public ApproxMaxKCutMutateConfig createConfig(CypherMapWrapper mapWrapper) {
        return ApproxMaxKCutMutateConfig.of(mapWrapper);
    }

    @Override
    public CypherMapWrapper createMinimalConfig(CypherMapWrapper mapWrapper) {
        if (!mapWrapper.containsKey("mutateProperty")) {
            mapWrapper = mapWrapper.withString("mutateProperty", this.mutateProperty());
        }

        return minimalConfigWithDefaults(mapWrapper);
    }

    // We override this in order to be able to specify an algo config yielding a deterministic result.
    @Override
    @Test
    public void testGraphMutation() {
        GraphStore graphStore = runMutation(ensureGraphExists(), createMinimalConfig(CypherMapWrapper.empty()));

        TestSupport.assertGraphEquals(fromGdl(expectedMutatedGraph()), graphStore.getUnion());
        GraphSchema schema = graphStore.schema();
        if (mutateProperty() != null) {
            boolean nodesContainMutateProperty = containsMutateProperty(schema.nodeSchema());
            boolean relationshipsContainMutateProperty = containsMutateProperty(schema.relationshipSchema());
            assertTrue(nodesContainMutateProperty || relationshipsContainMutateProperty);
        }
    }

    @Override
    public String expectedMutatedGraph() {
        return
            "  (a { community: 1 })" +
            ", (b { community: 1 })" +
            ", (c { community: 1 })" +
            ", (d { community: 0 })" +
            ", (e { community: 0 })" +
            ", (f { community: 0 })" +
            ", (g { community: 0 })" +

            ", (a)-[{w: 1.0d}]->(b)" +
            ", (a)-[{w: 1.0d}]->(d)" +
            ", (b)-[{w: 1.0d}]->(d)" +
            ", (b)-[{w: 1.0d}]->(e)" +
            ", (b)-[{w: 1.0d}]->(f)" +
            ", (b)-[{w: 1.0d}]->(g)" +
            ", (c)-[{w: 1.0d}]->(b)" +
            ", (c)-[{w: 1.0d}]->(e)" +
            ", (d)-[{w: 1.0d}]->(c)" +
            ", (d)-[{w: 1.0d}]->(b)" +
            ", (e)-[{w: 1.0d}]->(b)" +
            ", (f)-[{w: 1.0d}]->(a)" +
            ", (f)-[{w: 1.0d}]->(b)" +
            ", (g)-[{w: 1.0d}]->(b)" +
            ", (g)-[{w: 1.0d}]->(c)";
    }
}
