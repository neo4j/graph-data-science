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
package org.neo4j.gds.junit.annotation;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.neo4j.gds.core.GdsEdition;

public class GdsEditionTestExtension implements
    BeforeTestExecutionCallback,
    AfterTestExecutionCallback,
    BeforeEachCallback,
    AfterEachCallback,
    BeforeAllCallback,
    AfterAllCallback {

    private static final String EDITION_CONTEXT_STORE_KEY = "is_enterprise";

    @Override
    public void beforeAll(ExtensionContext context) {
        setGdsEditionAtClassLevel(context);
    }

    @Override
    public void afterAll(ExtensionContext context) {
        resetGdsEditionAtClassLevel(context);
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        setGdsEditionAtClassLevel(context);
    }

    @Override
    public void afterEach(ExtensionContext context) {
        resetGdsEditionAtClassLevel(context);
    }

    @Override
    public void beforeTestExecution(ExtensionContext context) {
        if (isMethodAnnotation(context)) {
            var namespace = namespace(context, false);

            var testClass = context.getRequiredTestMethod();
            var gdsEdition = testClass.getAnnotation(GdsEditionTest.class).value();

            setEdition(namespace, gdsEdition, context);
        }
    }

    @Override
    public void afterTestExecution(ExtensionContext context) {
        if (isMethodAnnotation(context)) {
            var namespace = namespace(context, false);
            resetEdition(namespace, context);
        }
    }

    private void setEdition(ExtensionContext.Namespace namespace, Edition edition, ExtensionContext context) {
        context
            .getStore(namespace)
            .put(EDITION_CONTEXT_STORE_KEY, GdsEdition.instance().isOnEnterpriseEdition());

        switch (edition) {
            case CE:
                GdsEdition.instance().setToCommunityEdition();
                break;
            case EE:
                GdsEdition.instance().setToEnterpriseEdition();
                break;
        }
    }

    private void resetEdition(ExtensionContext.Namespace namespace, ExtensionContext context) {
        var contextStore = context.getStore(namespace);
        boolean wasEnterprisePreviously = (boolean) contextStore.get(EDITION_CONTEXT_STORE_KEY);

        if (wasEnterprisePreviously) {
            GdsEdition.instance().setToEnterpriseEdition();
        } else {
            GdsEdition.instance().setToCommunityEdition();
        }
    }

    private void setGdsEditionAtClassLevel(ExtensionContext context) {
        if (isClassAnnotation(context)) {
            var namespace = namespace(context, true);

            var testClass = context.getRequiredTestClass();
            var gdsEdition = testClass.getAnnotation(GdsEditionTest.class).value();

            setEdition(namespace, gdsEdition, context);
        }
    }

    private void resetGdsEditionAtClassLevel(ExtensionContext context) {
        if (isClassAnnotation(context)) {
            var namespace = namespace(context, true);
            resetEdition(namespace, context);
        }
    }

    private boolean isClassAnnotation(ExtensionContext context) {
        return context.getRequiredTestClass().getAnnotation(GdsEditionTest.class) != null;
    }

    private boolean isMethodAnnotation(ExtensionContext context) {
        return context.getRequiredTestMethod().getAnnotation(GdsEditionTest.class) != null;
    }

    private ExtensionContext.Namespace namespace(ExtensionContext context, boolean global) {
        var contextObject = global
            ? context.getRequiredTestClass()
            : context.getRequiredTestMethod();

        return ExtensionContext.Namespace.create(contextObject);
    }
}
