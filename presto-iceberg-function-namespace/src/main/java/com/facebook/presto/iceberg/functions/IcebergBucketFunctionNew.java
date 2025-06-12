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
import com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.JavaScalarFunctionImplementation;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunctionVisibility;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.iceberg.functions.IcebergFunctionNamespaceManager.ICEBERG_BUILTIN_NAMESPACE;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static com.facebook.presto.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static com.facebook.presto.spi.function.Signature.typeVariable;
import static com.facebook.presto.spi.function.SqlFunctionVisibility.PUBLIC;
import static com.facebook.presto.util.Reflection.methodHandle;

public class IcebergBucketFunctionNew
        extends IcebergScalarFunction
{
    public static final IcebergBucketFunctionNew BUCKET_FUNCTION_NEW = new IcebergBucketFunctionNew();
    private static final String NAME = "bucketnew";
    private static final MethodHandle BUCKET_INTEGER = methodHandle(IcebergBucketFunctionNew.class, "bucketInteger", long.class, long.class);

    public IcebergBucketFunctionNew()
    {
        super(new Signature(QualifiedObjectName.valueOf(ICEBERG_BUILTIN_NAMESPACE, NAME),
                SCALAR,
                ImmutableList.of(typeVariable("T")),
                ImmutableList.of(),
                parseTypeSignature(StandardTypes.INTEGER),
                ImmutableList.of(parseTypeSignature("T"),
                        parseTypeSignature(StandardTypes.INTEGER)),
                false),
                "Iceberg bucket partition transform function",
                true,
                false);
    }

    @Override
    public String getDescription()
    {
        return "Iceberg bucket partition transform function";
    }

    @Override
    public FunctionMetadata getFunctionMetadata()
    {
        return null;
    }

    @Override
    public JavaScalarFunctionImplementation getScalarFunctionImplementation(BoundVariables boundVariables, int arity)
    {
        Type type = boundVariables.getTypeVariable("T");
        return generateScalarFunctionImplementation(type);
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
