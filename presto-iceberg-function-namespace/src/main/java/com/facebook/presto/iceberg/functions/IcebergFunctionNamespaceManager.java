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

import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.function.SqlFunctionResult;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.UserDefinedType;
import com.facebook.presto.metadata.BuiltInFunctionHandle;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.metadata.SpecializedFunctionKey;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.AlterRoutineCharacteristics;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.FunctionNamespaceManager;
import com.facebook.presto.spi.function.FunctionNamespaceTransactionHandle;
import com.facebook.presto.spi.function.Parameter;
import com.facebook.presto.spi.function.ScalarFunctionImplementation;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunction;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.function.SqlInvokedScalarFunctionImplementation;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.util.concurrent.UncheckedExecutionException;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.common.function.OperatorType.tryGetOperatorType;
import static com.facebook.presto.iceberg.functions.IcebergBucketFunction.BUCKET_FUNCTION;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static com.facebook.presto.spi.function.FunctionImplementationType.JAVA;
import static com.facebook.presto.spi.function.FunctionImplementationType.SQL;
import static com.facebook.presto.spi.function.FunctionKind.AGGREGATE;
import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.HOURS;

@ThreadSafe
public class IcebergFunctionNamespaceManager
        implements FunctionNamespaceManager<SqlFunction>
{
    public static final CatalogSchemaName ICEBERG_BUILTIN_NAMESPACE = new CatalogSchemaName("iceberg", "default");
    public static final String ID = "iceberg-functions";

    private final FunctionAndTypeManager functionAndTypeManager;
    private final LoadingCache<Signature, SpecializedFunctionKey> specializedFunctionKeyCache;
    private final LoadingCache<SpecializedFunctionKey, ScalarFunctionImplementation> specializedScalarCache;

    private volatile FunctionMap functions = new FunctionMap();

    @Inject
    public IcebergFunctionNamespaceManager(
            FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");

        specializedFunctionKeyCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(1, HOURS)
                .build(CacheLoader.from(this::doGetSpecializedFunctionKey));

        // TODO the function map should be updated, so that this cast can be removed

        // We have observed repeated compilation of MethodHandle that leads to full GCs.
        // We notice that flushing the following caches mitigate the problem.
        // We suspect that it is a JVM bug that is related to stale/corrupted profiling data associated
        // with generated classes and/or dynamically-created MethodHandles.
        // This might also mitigate problems like deoptimization storm or unintended interpreted execution.

        specializedScalarCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(1, HOURS)
                .build(CacheLoader.from(key -> {
                    checkArgument(
                            key.getFunction() instanceof SqlScalarFunction || key.getFunction() instanceof SqlInvokedFunction,
                            "Unsupported scalar function class: %s",
                            key.getFunction().getClass());
                    return key.getFunction() instanceof SqlScalarFunction
                            ? ((SqlScalarFunction) key.getFunction()).specialize(key.getBoundVariables(), key.getArity(), functionAndTypeManager)
                            : new SqlInvokedScalarFunctionImplementation(((SqlInvokedFunction) key.getFunction()).getBody());
                }));

        registerIcebergFunctions(getIcebergFunctions());
    }

    private List<? extends SqlFunction> getIcebergFunctions()
    {
        FunctionListBuilder builder = new FunctionListBuilder()
                .function(BUCKET_FUNCTION);

        return builder.getFunctions();
    }

    public synchronized void registerIcebergFunctions(List<? extends SqlFunction> functions)
    {
        for (SqlFunction function : functions) {
            for (SqlFunction existingFunction : this.functions.list()) {
                checkArgument(!function.getSignature().equals(existingFunction.getSignature()), "Function already registered: %s", function.getSignature());
            }
        }
        this.functions = new FunctionMap(this.functions, functions);
    }

    @Override
    public void setBlockEncodingSerde(BlockEncodingSerde blockEncodingSerde)
    {
        // Do not need to do anything here since BlockEncodingSerde is passed in constructor
    }

    @Override
    public void createFunction(SqlInvokedFunction function, boolean replace)
    {
        throw new PrestoException(GENERIC_USER_ERROR, format("Cannot create function in built-in function namespace: %s", function.getSignature().getName()));
    }

    @Override
    public void alterFunction(QualifiedObjectName functionName, Optional<List<TypeSignature>> parameterTypes, AlterRoutineCharacteristics alterRoutineCharacteristics)
    {
        throw new PrestoException(GENERIC_USER_ERROR, format("Cannot alter function in built-in function namespace: %s", functionName));
    }

    @Override
    public void dropFunction(QualifiedObjectName functionName, Optional<List<TypeSignature>> parameterTypes, boolean exists)
    {
        throw new PrestoException(GENERIC_USER_ERROR, format("Cannot drop function in built-in function namespace: %s", functionName));
    }

    public String getName()
    {
        return ID;
    }

    @Override
    public FunctionNamespaceTransactionHandle beginTransaction()
    {
        return new EmptyTransactionHandle();
    }

    @Override
    public void commit(FunctionNamespaceTransactionHandle transactionHandle)
    {
    }

    @Override
    public void abort(FunctionNamespaceTransactionHandle transactionHandle)
    {
    }

    /**
     * likePattern / escape is not used for optimization, returning all functions.
     */
    @Override
    public Collection<SqlFunction> listFunctions(Optional<String> likePattern, Optional<String> escape)
    {
        return functions.list();
    }

    @Override
    public Collection<SqlFunction> getFunctions(Optional<? extends FunctionNamespaceTransactionHandle> transactionHandle, QualifiedObjectName functionName)
    {
        return functions.get(functionName);
    }

    @Override
    public FunctionHandle getFunctionHandle(Optional<? extends FunctionNamespaceTransactionHandle> transactionHandle, Signature signature)
    {
        return new BuiltInFunctionHandle(signature);
    }

    @Override
    public FunctionMetadata getFunctionMetadata(FunctionHandle functionHandle)
    {
        checkArgument(functionHandle instanceof BuiltInFunctionHandle, "Expect BuiltInFunctionHandle");
        Signature signature = ((BuiltInFunctionHandle) functionHandle).getSignature();
        SpecializedFunctionKey functionKey;
        try {
            functionKey = specializedFunctionKeyCache.getUnchecked(signature);
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), PrestoException.class);
            throw e;
        }
        SqlFunction function = functionKey.getFunction();
        Optional<OperatorType> operatorType = tryGetOperatorType(signature.getName());
        if (operatorType.isPresent()) {
            return new FunctionMetadata(
                    operatorType.get(),
                    signature.getArgumentTypes(),
                    signature.getReturnType(),
                    signature.getKind(),
                    JAVA,
                    function.isDeterministic(),
                    function.isCalledOnNullInput(),
                    function.getComplexTypeFunctionDescriptor());
        }
        else if (function instanceof SqlInvokedFunction) {
            SqlInvokedFunction sqlFunction = (SqlInvokedFunction) function;
            List<String> argumentNames = sqlFunction.getParameters().stream().map(Parameter::getName).collect(toImmutableList());
            return new FunctionMetadata(
                    signature.getName(),
                    signature.getArgumentTypes(),
                    argumentNames,
                    signature.getReturnType(),
                    signature.getKind(),
                    sqlFunction.getRoutineCharacteristics().getLanguage(),
                    SQL,
                    function.isDeterministic(),
                    function.isCalledOnNullInput(),
                    sqlFunction.getVersion(),
                    sqlFunction.getComplexTypeFunctionDescriptor());
        }
        else {
            return new FunctionMetadata(
                    signature.getName(),
                    signature.getArgumentTypes(),
                    signature.getReturnType(),
                    signature.getKind(),
                    JAVA,
                    function.isDeterministic(),
                    function.isCalledOnNullInput(),
                    function.getComplexTypeFunctionDescriptor());
        }
    }

    @Override
    public final CompletableFuture<SqlFunctionResult> executeFunction(String source, FunctionHandle functionHandle, Page input, List<Integer> channels, TypeManager typeManager)
    {
        throw new IllegalStateException("Builtin function execution should be handled by the engine.");
    }

    @Override
    public void addUserDefinedType(UserDefinedType userDefinedType)
    {
        throw new UnsupportedOperationException("User defined type is not supported");
    }

    @Override
    public Optional<UserDefinedType> getUserDefinedType(QualifiedObjectName typeName)
    {
        throw new UnsupportedOperationException("User defined type is not supported");
    }

    @Override
    public ScalarFunctionImplementation getScalarFunctionImplementation(FunctionHandle functionHandle)
    {
        checkArgument(functionHandle instanceof BuiltInFunctionHandle, "Expect BuiltInFunctionHandle");
        return getScalarFunctionImplementation(((BuiltInFunctionHandle) functionHandle).getSignature());
    }

    public ScalarFunctionImplementation getScalarFunctionImplementation(Signature signature)
    {
        checkArgument(signature.getKind() == SCALAR, "%s is not a scalar function", signature);
        checkArgument(signature.getTypeVariableConstraints().isEmpty(), "%s has unbound type parameters", signature);

        try {
            return specializedScalarCache.getUnchecked(getSpecializedFunctionKey(signature));
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), PrestoException.class);
            throw e;
        }
    }

    private SpecializedFunctionKey getSpecializedFunctionKey(Signature signature)
    {
        try {
            return specializedFunctionKeyCache.getUnchecked(signature);
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), PrestoException.class);
            throw e;
        }
    }

    private SpecializedFunctionKey doGetSpecializedFunctionKey(Signature signature)
    {
        return functionAndTypeManager.getSpecializedFunctionKey(signature);
    }

    private static class EmptyTransactionHandle
            implements FunctionNamespaceTransactionHandle
    {
    }

    private static class FunctionMap
    {
        private final Multimap<QualifiedObjectName, SqlFunction> functions;

        public FunctionMap()
        {
            functions = ImmutableListMultimap.of();
        }

        public FunctionMap(FunctionMap map, Iterable<? extends SqlFunction> functions)
        {
            this.functions = ImmutableListMultimap.<QualifiedObjectName, SqlFunction>builder()
                    .putAll(map.functions)
                    .putAll(Multimaps.index(functions, function -> function.getSignature().getName()))
                    .build();

            // Make sure all functions with the same name are aggregations or none of them are
            for (Map.Entry<QualifiedObjectName, Collection<SqlFunction>> entry : this.functions.asMap().entrySet()) {
                Collection<SqlFunction> values = entry.getValue();
                long aggregations = values.stream()
                        .map(function -> function.getSignature().getKind())
                        .filter(kind -> kind == AGGREGATE)
                        .count();
                checkState(aggregations == 0 || aggregations == values.size(), "'%s' is both an aggregation and a scalar function", entry.getKey());
            }
        }

        public List<SqlFunction> list()
        {
            return ImmutableList.copyOf(functions.values());
        }

        public Collection<SqlFunction> get(QualifiedObjectName name)
        {
            return functions.get(name);
        }
    }
}
