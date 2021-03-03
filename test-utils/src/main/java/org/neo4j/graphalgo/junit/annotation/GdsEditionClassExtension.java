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
package org.neo4j.graphalgo.junit.annotation;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class GdsEditionClassExtension extends GdsEditionExtension  implements BeforeEachCallback, AfterEachCallback {
    @Override
    public void afterEach(ExtensionContext context) {
        resetEdition(context);
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        setEdition(context);
    }

    private void setEdition(ExtensionContext context) {
        var contextStore = context.getStore(ExtensionContext.Namespace.GLOBAL);
        contextStore.put("isEnterprise", org.neo4j.graphalgo.core.GdsEdition.instance().isOnEnterpriseEdition());

        var testClass = context.getRequiredTestClass();
        var gdsEdition = testClass.getAnnotation(GdsEditionTestCase.class);

        setGdsEdition(gdsEdition.value());
    }
}
