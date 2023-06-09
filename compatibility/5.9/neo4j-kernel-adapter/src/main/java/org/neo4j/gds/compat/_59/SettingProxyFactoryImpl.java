/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file contains proprietary code that is only available via a commercial license from Neo4j.
 * For more information, see https://neo4j.com/contact-us/
 */
package org.neo4j.gds.compat._59;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.gds.compat.Neo4jVersion;
import org.neo4j.gds.compat.SettingProxyApi;
import org.neo4j.gds.compat.SettingProxyFactory;

@ServiceProvider
public final class SettingProxyFactoryImpl implements SettingProxyFactory {

    @Override
    public boolean canLoad(Neo4jVersion version) {
        return false;
    }

    @Override
    public SettingProxyApi load() {
        throw new UnsupportedOperationException("5.9 compatibility requires JDK17");
    }

    @Override
    public String description() {
        return "Neo4j Settings 5.9 (placeholder)";
    }
}
