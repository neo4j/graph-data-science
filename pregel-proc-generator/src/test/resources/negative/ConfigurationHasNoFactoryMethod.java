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
import org.neo4j.gds.beta.pregel.PregelComputation;
import org.neo4j.gds.beta.pregel.PregelProcedureConfig;
import org.neo4j.gds.beta.pregel.context.ComputeContext;
import org.neo4j.gds.beta.pregel.PregelSchema;
import org.neo4j.gds.beta.pregel.annotation.GDSMode;
import org.neo4j.gds.beta.pregel.annotation.PregelProcedure;
import org.neo4j.procedure.Description;

import java.util.Queue;

@PregelProcedure(name = "gds.pregel.test", modes = {GDSMode.STREAM})
@Description("Test computation description")
public class ConfigurationHasNoFactoryMethod implements PregelComputation<ConfigurationHasNoFactoryMethod.ComputationConfig> {

    @Override
    public PregelSchema schema() {
        return null;
    }

    @Override
    public void compute(ComputeContext<ConfigurationHasNoFactoryMethod.ComputationConfig> context, final long nodeId, Queue<Double> messages) {

    }

    interface ComputationConfig extends PregelProcedureConfig {

    }
}
