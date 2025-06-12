/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.presto.iceberg.functions;

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.function.AbstractFunctionAndTypeManager;
import com.facebook.presto.spi.function.FunctionNamespaceManager;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public class IcebergFunctionModule
        extends AbstractConfigurationAwareModule
{
    private final String catalogName;
    private final ClassLoader classLoader;

    private final AbstractFunctionAndTypeManager functionAndTypeManager;

    public IcebergFunctionModule(String catalogName, ClassLoader classLoader, AbstractFunctionAndTypeManager functionAndTypeManager)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        binder.bind(FunctionAndTypeManager.class).toInstance((FunctionAndTypeManager) functionAndTypeManager);
        binder.bind(FunctionNamespaceManager.class).to(IcebergFunctionNamespaceManager.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    @ForIcebergFunction
    public ClassLoader getClassLoader()
    {
        return classLoader;
    }

    @Provides
    public BlockEncodingSerde provideBlockEncodingSerde()
    {
        return new BlockEncodingManager();
    }

    @Provides
    public Set<Type> provideTypes()
    {
        return ImmutableSet.of();
    }
}
