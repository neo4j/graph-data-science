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
package gds.example;

import org.neo4j.graphalgo.annotation.Configuration;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.beta.pregel.annotation.GDSMode;
import org.neo4j.graphalgo.beta.pregel.annotation.PregelProcedure;
import org.neo4j.graphalgo.beta.pregel.context.ComputeContext;
import org.neo4j.graphalgo.beta.pregel.context.InitContext;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;

import java.util.Optional;

@PregelProcedure(name = "pregel.example", modes = {GDSMode.STREAM}, description = "My first Pregel example")
public class ExamplePregelComputation implements PregelComputation<ExamplePregelComputation.ExampleConfig> {

    public static final String KEY = "key";

    @Override
    public PregelSchema schema(ExampleConfig config) {
        // Declare a node schema with a single node value of type Long
        return new PregelSchema.Builder().add(KEY, ValueType.LONG).build();
    }

    @Override
    public void init(InitContext<ExampleConfig> context) {
        // Set node identifier as initial node value
        context.setNodeValue(KEY, context.nodeId());
    }

    @Override
    public void compute(ComputeContext<ExampleConfig> context, Messages messages) {
        // Silence is golden
        context.voteToHalt();
    }

    @ValueClass
    @Configuration
    @SuppressWarnings("immutables:subtype")
    public interface ExampleConfig extends PregelProcedureConfig {

        static ExamplePregelComputation.ExampleConfig of(
                String username,
                Optional<String> graphName,
                Optional<GraphCreateConfig> maybeImplicitCreate,
                CypherMapWrapper userConfig
        ) {
            return new ExampleConfigImpl(graphName, maybeImplicitCreate, username, userConfig);
        }
    }
}
