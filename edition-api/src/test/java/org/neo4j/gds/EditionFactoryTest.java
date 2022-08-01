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
package org.neo4j.gds;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Procedure;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;

@ImpermanentDbmsExtension(configurationCallback = "configuration")
class EditionFactoryTest {

    @Inject
    GraphDatabaseService db;

    @ExtensionCallback
    void configuration(TestDatabaseManagementServiceBuilder builder) {
        builder
            .noOpSystemGraphInitializer()
            .removeExtensions(it -> it instanceof EditionFactory)
            .addExtension(new EditionFactory());
    }

    @Test
    void shouldInjectModelCatalog() throws Exception {
        GraphDatabaseApiProxy.registerProcedures(db, Proc.class);
        db.executeTransactionally("CALL test_proc()");
    }

    public static class Proc {
        @Context
        public ModelCatalog modelCatalog;

        @Procedure("test_proc")
        public void run() {
            assertThat(modelCatalog).isNotNull();
        }
    }
}
