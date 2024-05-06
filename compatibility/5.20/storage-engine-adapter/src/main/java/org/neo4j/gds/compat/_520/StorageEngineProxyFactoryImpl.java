/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file contains proprietary code that is only available via a commercial license from Neo4j.
 * For more information, see https://neo4j.com/contact-us/
 */
package org.neo4j.gds.compat._520;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.gds.compat.Neo4jVersion;
import org.neo4j.gds.compat.StorageEngineProxyApi;
import org.neo4j.gds.compat.StorageEngineProxyFactory;

@ServiceProvider
public class StorageEngineProxyFactoryImpl implements StorageEngineProxyFactory {

    @Override
    public boolean canLoad(Neo4jVersion version) {
        return false;
    }

    @Override
    public StorageEngineProxyApi load() {
        throw new UnsupportedOperationException("RC storage engine requires JDK17 or later");
    }

    @Override
    public String description() {
        return "Storage Engine RC";
    }
}
