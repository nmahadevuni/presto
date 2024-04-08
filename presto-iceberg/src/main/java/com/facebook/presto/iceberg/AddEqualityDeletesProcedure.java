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
package com.facebook.presto.iceberg;

import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.procedure.Procedure;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;
import javax.inject.Provider;

import java.io.IOException;
import java.lang.invoke.MethodHandle;

import static com.facebook.presto.common.block.MethodHandleUtil.methodHandle;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class AddEqualityDeletesProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle ADD_EQUALITY_DELETES = methodHandle(
            AddEqualityDeletesProcedure.class,
            "addEqualityDeletes",
            ConnectorSession.class,
            String.class,
            String.class,
            String.class);
    private final IcebergMetadataFactory metadataFactory;
    private final HdfsEnvironment hdfsEnvironment;

    @Inject
    public AddEqualityDeletesProcedure(
            IcebergMetadataFactory metadataFactory,
            HdfsEnvironment hdfsEnvironment)
    {
        this.metadataFactory = requireNonNull(metadataFactory, "metadataFactory is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "add_equality_deletes",
                ImmutableList.of(
                        new Procedure.Argument("schemaTable", VARCHAR),
                        new Procedure.Argument("deleteFilesSchemaTable", VARCHAR),
                        new Procedure.Argument("deleteColumnNames", VARCHAR)),
                ADD_EQUALITY_DELETES.bindTo(this));
    }

    public void addEqualityDeletes(ConnectorSession clientSession, String schemaTable, String deleteFilesSchemaTable, String deleteColumnNames)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            IcebergAbstractMetadata metadata = (IcebergAbstractMetadata) metadataFactory.create();
            SchemaTableName schemaTableName = SchemaTableName.valueOf(schemaTable);
            if (!metadata.schemaExists(clientSession, schemaTableName.getSchemaName())) {
                throw new SchemaNotFoundException(schemaTableName.getSchemaName());
            }

            SchemaTableName deleteFileSchemaTableName = SchemaTableName.valueOf(deleteFilesSchemaTable);
            if (!metadata.schemaExists(clientSession, schemaTableName.getSchemaName())) {
                throw new SchemaNotFoundException(schemaTableName.getSchemaName());
            }

            metadata.addEqualityDeletes(clientSession, schemaTableName, deleteFileSchemaTableName, deleteColumnNames, hdfsEnvironment);
        }
        catch (IOException e) {
            throw new PrestoException(ICEBERG_FILESYSTEM_ERROR, format("Error while adding equality deletes: %s", e.getMessage()), e);
        }
    }
}
