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
package org.neo4j.gds.extension;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.core.model.ModelCatalog;

import java.util.Optional;

import static org.neo4j.gds.extension.ExtensionUtil.injectInstance;

public class Neo4jModelCatalogResolver implements BeforeEachCallback, AfterEachCallback {

    // taken from org.neo4j.test.extension.DbmsSupportController
    private static final ExtensionContext.Namespace DBMS_NAMESPACE = ExtensionContext.Namespace.create(
        "org",
        "neo4j",
        "dbms"
    );

    // taken from org.neo4j.test.extension.DbmsSupportController
    private static final String DBMS_KEY = "service";

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        injectFields(context, getModelCatalog(context));
    }

    private ModelCatalog getModelCatalog(ExtensionContext context) {
        var db = getDbms(context)
            .map(dbms -> dbms.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME))
            .orElseThrow(() -> new IllegalStateException("No database was found."));

        return GraphDatabaseApiProxy.resolveDependency(db, ModelCatalog.class);
    }

    private Optional<DatabaseManagementService> getDbms(ExtensionContext context) {
        return Optional.ofNullable(context.getStore(DBMS_NAMESPACE).get(DBMS_KEY, DatabaseManagementService.class));
    }

    private void injectFields(ExtensionContext context, ModelCatalog modelCatalog) {
        context.getRequiredTestInstances().getAllInstances().forEach(testInstance -> {
            injectInstance(
                testInstance,
                modelCatalog,
                ModelCatalog.class
            );
        });
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        getModelCatalog(context).removeAllLoadedModels();
    }
}
