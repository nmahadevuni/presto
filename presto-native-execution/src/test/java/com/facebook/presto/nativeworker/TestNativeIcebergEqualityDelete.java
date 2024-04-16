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
package com.facebook.presto.nativeworker;

import com.facebook.presto.Session;
import com.facebook.presto.hive.HdfsConfiguration;
import com.facebook.presto.hive.HdfsConfigurationInitializer;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveHdfsConfiguration;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.file.FileHiveMetastore;
import com.facebook.presto.iceberg.IcebergUtil;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableSet;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateProperties;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Path;

import static com.facebook.presto.hive.metastore.InMemoryCachingHiveMetastore.memoizeMetastore;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static java.lang.String.format;

public class TestNativeIcebergEqualityDelete
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.createNativeIcebergQueryRunner(true, "PARQUET");
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.createJavaIcebergQueryRunner("PARQUET");
    }

    protected void createTableWithMergeOnRead(Session session, String schema, String tableName)
    {
        assertUpdate("CREATE TABLE " + tableName + " (c1 int , c2 int) WITH (format_version = '2')");

        CatalogManager catalogManager = getDistributedQueryRunner().getCoordinator().getCatalogManager();
        ConnectorId connectorId = catalogManager.getCatalog(ICEBERG_CATALOG).get().getConnectorId();

        Table icebergTable = getIcebergTable(session.toConnectorSession(connectorId), schema, tableName);

        UpdateProperties updateProperties = icebergTable.updateProperties();
        updateProperties.set("write.merge.mode", "merge-on-read");
        updateProperties.commit();
    }

    @Test
    public void testEqDelete()
    {
        tearDown();
        createTableWithMergeOnRead(getSession(), "tpch", "test_eq_delete");
        assertUpdateExpected(getSession(), "INSERT INTO test_eq_delete VALUES (1, 2), (2, 3), (3,4)", 3);
        assertQuery("SELECT * FROM test_eq_delete", "VALUES(1, 2), (2, 3), (3, 4)");

        String tableLocation = getLocation("catalog", "test_eq_delete");
        System.out.println("Table location " + tableLocation);

        assertUpdateExpected(getSession(), "CREATE TABLE delete_file as SELECT * from test_eq_delete WHERE c1 = 3", 1);

        assertUpdateExpected(getSession(), format("CALL system.add_equality_deletes('%s','%s','%s')", "tpch.test_eq_delete", "tpch.delete_file", "c1"));

        Session icebergQuerySession = Session.builder(getSession())
                .setCatalogSessionProperty(
                        ICEBERG_CATALOG,
                        "delete_as_join_rewrite_enabled",
                        "false")
                .build();
        assertQueryWithSameExpectedQueryRunner("SELECT * FROM test_eq_delete", "VALUES(1, 2), (2,3)");
        // Once the native worker supports reading equality deletes, we can enable the below test and remove above test.
        // assertQuery(icebergQuerySession, "SELECT * FROM test_eq_delete", "VALUES(1, 2), (2,3)");
    }

    protected String getLocation(String catalog, String table)
    {
        File tempLocation = ((DistributedQueryRunner) getQueryRunner()).getCoordinator().getDataDirectory().toFile();
        return format("%s/%s/tpch/%s", tempLocation.getPath(), catalog, table);
    }

    protected static HdfsEnvironment getHdfsEnvironment()
    {
        HiveClientConfig hiveClientConfig = new HiveClientConfig();
        MetastoreClientConfig metastoreClientConfig = new MetastoreClientConfig();
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hiveClientConfig, metastoreClientConfig),
                ImmutableSet.of(),
                hiveClientConfig);
        return new HdfsEnvironment(hdfsConfiguration, metastoreClientConfig, new NoHdfsAuthentication());
    }

    protected Path getCatalogDirectory()
    {
        Path dataDirectory = getDistributedQueryRunner().getCoordinator().getDataDirectory().resolve("catalog");
        return dataDirectory;
    }

    protected ExtendedHiveMetastore getFileHiveMetastore()
    {
        FileHiveMetastore fileHiveMetastore = new FileHiveMetastore(getHdfsEnvironment(),
                getCatalogDirectory().toFile().getPath(),
                "test");
        return memoizeMetastore(fileHiveMetastore, false, 1000, 0);
    }

    protected Table getIcebergTable(ConnectorSession session, String schema, String tableName)
    {
        return IcebergUtil.getHiveIcebergTable(getFileHiveMetastore(),
                getHdfsEnvironment(),
                session,
                SchemaTableName.valueOf(schema + "." + tableName));
    }

    @AfterClass
    protected void tearDown()
    {
        assertUpdate("DROP TABLE IF EXISTS TEST_EQ_DELETE");
        assertUpdate("DROP TABLE IF EXISTS DELETE_FILE");
        assertUpdate("DROP TABLE IF EXISTS TEST_POS_DELETE");
    }
}
