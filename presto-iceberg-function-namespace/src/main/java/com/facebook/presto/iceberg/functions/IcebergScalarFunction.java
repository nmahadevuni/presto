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

import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.spi.function.JavaScalarFunctionImplementation;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunctionVisibility;

import static com.facebook.presto.spi.function.SqlFunctionVisibility.PUBLIC;

public abstract class IcebergScalarFunction
        extends IcebergFunction
{
    protected IcebergScalarFunction(Signature signature,
                                  String description,
                                  boolean isDeterministic,
                                  boolean isCalledOnNullInput)
    {
        super(signature.getName(),
                signature,
                false,
                isDeterministic,
                isCalledOnNullInput,
                description);
    }

    public abstract JavaScalarFunctionImplementation getScalarFunctionImplementation(BoundVariables boundVariables, int arity);

    @Override
    public SqlFunctionVisibility getVisibility()
    {
        return PUBLIC;
    }
}
