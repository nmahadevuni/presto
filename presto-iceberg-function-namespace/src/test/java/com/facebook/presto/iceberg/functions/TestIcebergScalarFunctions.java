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

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.testing.MaterializedResult;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Optional;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.google.common.math.DoubleMath.fuzzyEquals;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@SuppressWarnings("UnknownLanguage")
public class TestIcebergScalarFunctions
        extends AbstractTestIcebergFunctions
{
    private static final String FUNCTION_PREFIX = "iceberg.default.";
    private static final String TABLE_NAME = "memory.default.function_testing";
    private static final Type INTEGER_ARRAY = new ArrayType(INTEGER);
    private static final Type VARCHAR_ARRAY = new ArrayType(VARCHAR);

    @Override
    protected Optional<File> getInitScript()
    {
        return Optional.of(new File("src/test/sql/function-testing.sql"));
    }

    @Test
    public void genericFunction()
    {
        check(select("bucket", "5", "16"), INTEGER, 5);
    }

    public void check(@Language("SQL") String query, Type expectedType, Object expectedValue)
    {
        MaterializedResult result = client.execute(query).getResult();
        assertEquals(result.getRowCount(), 1);
        assertEquals(result.getTypes().get(0), expectedType);
        Object actual = result.getMaterializedRows().get(0).getField(0);

        if (expectedType.equals(DOUBLE) || expectedType.equals(RealType.REAL)) {
            if (expectedValue == null) {
                assertNaN(actual);
            }
            else {
                assertTrue(fuzzyEquals(((Number) actual).doubleValue(), ((Number) expectedValue).doubleValue(), 0.000001));
            }
        }
        else {
            assertEquals(actual, expectedValue);
        }
    }

    private Type typeOf(String signature)
    {
        return typeManager.getType(parseTypeSignature(signature));
    }

    private static void assertNaN(Object o)
    {
        if (o instanceof Double) {
            assertEquals(((Double) o).doubleValue(), Double.NaN);
        }
        else if (o instanceof Float) {
            assertEquals(((Float) o).floatValue(), Float.NaN);
        }
        else {
            fail("Unexpected " + o);
        }
    }

    private static String select(String function, String... args)
    {
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT ").append(FUNCTION_PREFIX).append(function);
        builder.append("(");
        if (args != null) {
            builder.append(String.join(", ", args));
        }
        builder.append(")");
        return builder.toString();
    }

    private static String selectF(String function, String... args)
    {
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT ").append(FUNCTION_PREFIX).append(function);
        builder.append("(");
        if (args != null) {
            builder.append(String.join(", ", args));
        }
        builder.append(")").append(" FROM ").append(TABLE_NAME);
        return builder.toString();
    }
}
