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
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.neo4j.graphalgo.core.GdsEdition;

public class GdsEnterpriseEditionExtension implements BeforeTestExecutionCallback, AfterTestExecutionCallback, BeforeEachCallback, AfterEachCallback {

    @Override
    public void beforeTestExecution(ExtensionContext context) {
        GdsEdition.instance().setToEnterpriseEdition();
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        GdsEdition.instance().setToEnterpriseEdition();
    }

    @Override
    public void afterTestExecution(ExtensionContext context) {
        GdsEdition.instance().setToCommunityEdition();
    }

    @Override
    public void afterEach(ExtensionContext context) {
        GdsEdition.instance().setToCommunityEdition();
    }

}
