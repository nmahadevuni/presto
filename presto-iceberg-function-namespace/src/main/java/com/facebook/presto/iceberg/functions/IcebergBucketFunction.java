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

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunctionVisibility;
import com.google.common.collect.ImmutableList;
//import org.apache.iceberg.transforms.Transforms;
//import org.apache.iceberg.types.Types;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.iceberg.functions.IcebergFunctionNamespaceManager.ICEBERG_BUILTIN_NAMESPACE;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static com.facebook.presto.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static com.facebook.presto.spi.function.Signature.typeVariable;
import static com.facebook.presto.spi.function.SqlFunctionVisibility.PUBLIC;
import static com.facebook.presto.util.Reflection.methodHandle;

public class IcebergBucketFunction
        extends SqlScalarFunction
{
    public static final IcebergBucketFunction BUCKET_FUNCTION = new IcebergBucketFunction();
    private static final String NAME = "bucket";
    private static final MethodHandle BUCKET_INTEGER = methodHandle(IcebergBucketFunction.class, "bucketInteger", long.class, long.class);

    public IcebergBucketFunction()
    {
        super(new Signature(QualifiedObjectName.valueOf(ICEBERG_BUILTIN_NAMESPACE, NAME),
                SCALAR,
                ImmutableList.of(typeVariable("T")),
                ImmutableList.of(),
                parseTypeSignature(StandardTypes.INTEGER),
                ImmutableList.of(parseTypeSignature("T"),
                        parseTypeSignature(StandardTypes.INTEGER)),
                false));
    }

    @Override
    public String getDescription()
    {
        return "Iceberg bucket partition transform function";
    }

    @Override
    public final SqlFunctionVisibility getVisibility()
    {
        return PUBLIC;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public BuiltInScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        Type type = boundVariables.getTypeVariable("T");
        return generateScalarFunctionImplementation(type);
    }

    private static BuiltInScalarFunctionImplementation generateScalarFunctionImplementation(Type type)
    {
        MethodHandle methodHandle = null;
        if (type instanceof IntegerType || type instanceof TinyintType || type instanceof SmallintType || type instanceof BigintType) {
            methodHandle = BUCKET_INTEGER;
        }

        return new BuiltInScalarFunctionImplementation(
                false,
                ImmutableList.of(valueTypeArgumentProperty(NEVER_NULL), valueTypeArgumentProperty(NEVER_NULL)),
                methodHandle);
    }

    public static long bucketInteger(long value, long numberOfBuckets)
    {
//        return Transforms.bucket((int) numberOfBuckets)
//                .bind(Types.LongType.get())
//                .apply(value);
        return value % numberOfBuckets;
    }
}
