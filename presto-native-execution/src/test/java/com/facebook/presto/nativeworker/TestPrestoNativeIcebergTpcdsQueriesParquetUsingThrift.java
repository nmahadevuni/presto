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
import com.facebook.presto.iceberg.CommitTaskData;
import com.facebook.presto.iceberg.FileFormat;
import com.facebook.presto.iceberg.IcebergUtil;
import com.facebook.presto.iceberg.MetricsWrapper;
import com.facebook.presto.iceberg.PartitionData;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.types.Type;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.hive.metastore.CachingHiveMetastore.memoizeMetastore;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.parquet.ParquetReaderUtils.getParquetFileRowCount;
import static java.lang.String.format;
import static org.testng.Assert.assertTrue;

@Test(groups = {"parquet"})
public class TestPrestoNativeIcebergTpcdsQueriesParquetUsingThrift
        extends AbstractTestNativeTpcdsQueries
{
    String[] tpcdsTableNames;
    Map<String, List<String>> tableToDeleteTableMap;
    Map<String, Integer> deleteTableRecordCount = new HashMap<>();
    Map<String, String> deleteTableToColumnMap;

    TestPrestoNativeIcebergTpcdsQueriesParquetUsingThrift()
    {
        storageFormat = "PARQUET";
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.createNativeIcebergQueryRunner(true, storageFormat);
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.createJavaIcebergQueryRunner(storageFormat);
    }

    @Override
    protected void createTables()
    {
        tpcdsTableNames = new String[] {"call_center", "catalog_page", "catalog_returns", "catalog_sales",
                "customer", "customer_address", "customer_demographics", "date_dim", "household_demographics",
                "income_band", "inventory", "item", "promotion", "reason", "ship_mode", "store", "store_returns",
                "store_sales", "time_dim", "warehouse", "web_page", "web_returns", "web_sales", "web_site", "DF_I_1", "DF_I_2",
                "DF_I_3", "DF_CR_1", "DF_CR_2", "DF_CR_3", "DF_CS_1", "DF_CS_2", "DF_CS_3", "DF_SR_1", "DF_SR_2",
                "DF_SR_3", "DF_SS_1", "DF_SS_2", "DF_SS_3", "DF_WR_1", "DF_WR_2", "DF_WR_3", "DF_WS_1", "DF_WS_2",
                "DF_WS_3"};
        tableToDeleteTableMap = new ImmutableMap.Builder<String, List<String>>()
                .put("inventory", Arrays.asList("DF_I_1", "DF_I_2", "DF_I_3"))
                .put("catalog_returns", Arrays.asList("DF_CR_1", "DF_CR_2", "DF_CR_3"))
                .put("catalog_sales", Arrays.asList("DF_CS_1", "DF_CS_2", "DF_CS_3"))
                .put("store_returns", Arrays.asList("DF_SR_1", "DF_SR_2", "DF_SR_3"))
                .put("store_sales", Arrays.asList("DF_SS_1", "DF_SS_2", "DF_SS_3"))
                .put("web_returns", Arrays.asList("DF_WR_1", "DF_WR_2", "DF_WR_3"))
                .put("web_sales", Arrays.asList("DF_WS_1", "DF_WS_2", "DF_WS_3"))
                .build();
        deleteTableToColumnMap = new ImmutableMap.Builder<String, String>()
                .put("DF_I_1", "inv_date_sk")
                .put("DF_I_2", "inv_date_sk")
                .put("DF_I_3", "inv_date_sk")
                .put("DF_CR_1", "cr_order_number")
                .put("DF_CR_2", "cr_order_number")
                .put("DF_CR_3", "cr_order_number")
                .put("DF_CS_1", "cs_sold_date_sk")
                .put("DF_CS_2", "cs_sold_date_sk")
                .put("DF_CS_3", "cs_sold_date_sk")
                .put("DF_SR_1", "sr_ticket_number")
                .put("DF_SR_2", "sr_ticket_number")
                .put("DF_SR_3", "sr_ticket_number")
                .put("DF_SS_1", "ss_sold_date_sk")
                .put("DF_SS_2", "ss_sold_date_sk")
                .put("DF_SS_3", "ss_sold_date_sk")
                .put("DF_WR_1", "wr_order_number")
                .put("DF_WR_2", "wr_order_number")
                .put("DF_WR_3", "wr_order_number")
                .put("DF_WS_1", "ws_sold_date_sk")
                .put("DF_WS_2", "ws_sold_date_sk")
                .put("DF_WS_3", "ws_sold_date_sk")
                .build();
        super.createTables();
        createDeleteFiles(storageFormat);
    }

    private void createDeleteFiles(String storageFormat)
    {
        assertUpdateExpected(session, "CREATE TABLE DF_I_1 as " +
                "SELECT * " +
                "FROM inventory " +
                "WHERE  inv_date_sk >= (SELECT Min(d_date_sk) " +
                "FROM   date_dim " +
                "WHERE  d_date BETWEEN date'2000-05-18' AND date'2000-05-25') " +
                "AND inv_date_sk <= (SELECT Max(d_date_sk) " +
                "FROM   date_dim " +
                "WHERE  d_date BETWEEN date'2000-05-18' AND date'2000-05-25') ", 2002L);
        deleteTableRecordCount.put("DF_I_1", 2002);

        assertUpdateExpected(session, "CREATE TABLE DF_I_2 as " +
                "SELECT * " +
                "FROM inventory " +
                "WHERE  inv_date_sk >= (SELECT Min(d_date_sk) " +
                "                       FROM   date_dim " +
                "                       WHERE  d_date BETWEEN date'1999-09-16' AND date'1999-09-23') " +
                "       AND inv_date_sk <= (SELECT Max(d_date_sk) " +
                "                           FROM   date_dim " +
                "                           WHERE  d_date BETWEEN date'1999-09-16' AND date'1999-09-23') ", 2002L);
        deleteTableRecordCount.put("DF_I_2", 2002);

        assertUpdateExpected(session, "CREATE TABLE DF_I_3 as " +
                "SELECT * " +
                "FROM inventory " +
                "WHERE  inv_date_sk >= (SELECT Min(d_date_sk) " +
                "                       FROM   date_dim " +
                "                       WHERE  d_date BETWEEN date'2002-11-14' AND date'2002-11-21') " +
                "       AND inv_date_sk <= (SELECT Max(d_date_sk) " +
                "                           FROM   date_dim " +
                "                           WHERE  d_date BETWEEN date'2002-11-14' AND date'2002-11-21') ", 2002L);
        deleteTableRecordCount.put("DF_I_3", 2002);

        assertUpdateExpected(session, "CREATE TABLE DF_CR_1 as " +
                        "SELECT * " +
                        "FROM catalog_returns " +
                        "WHERE  cr_order_number IN (SELECT cs_order_number " +
                        "                           FROM   catalog_sales, " +
                        "                                  date_dim " +
                        "                           WHERE  cs_sold_date_sk = d_date_sk " +
                        "                                  AND d_date BETWEEN date'2000-05-20' AND " +
                        "                                                     date'2000-05-21') ",
                7L);
        deleteTableRecordCount.put("DF_CR_1", 7);

        assertUpdateExpected(session, "CREATE TABLE DF_CR_2 as " +
                        "SELECT * " +
                        "FROM catalog_returns " +
                        "WHERE  cr_order_number IN (SELECT cs_order_number " +
                        "                           FROM   catalog_sales, " +
                        "                                  date_dim " +
                        "                           WHERE  cs_sold_date_sk = d_date_sk " +
                        "                                  AND d_date BETWEEN date'1999-09-18' AND " +
                        "                                                     date'1999-09-19') ",
                12L);
        deleteTableRecordCount.put("DF_CR_2", 12);

        assertUpdateExpected(session, "CREATE TABLE DF_CR_3 as " +
                        "SELECT * " +
                        "FROM catalog_returns " +
                        "WHERE  cr_order_number IN (SELECT cs_order_number " +
                        "                           FROM   catalog_sales, " +
                        "                                  date_dim " +
                        "                           WHERE  cs_sold_date_sk = d_date_sk " +
                        "                                  AND d_date BETWEEN date'2002-11-12' AND " +
                        "                                                     date'2002-11-13') ",
                15L);
        deleteTableRecordCount.put("DF_CR_3", 15);

        assertUpdateExpected(session, "CREATE TABLE DF_CS_1 as " +
                        "SELECT * " +
                        "FROM catalog_sales " +
                        "WHERE  cs_sold_date_sk >= (SELECT Min(d_date_sk) " +
                        "                           FROM   date_dim " +
                        "                           WHERE  d_date BETWEEN date'2000-05-20' AND date'2000-05-21') " +
                        "       AND cs_sold_date_sk <= (SELECT Max(d_date_sk) " +
                        "                               FROM   date_dim " +
                        "                               WHERE  d_date BETWEEN date'2000-05-20' AND " +
                        "                                                     date'2000-05-21') ",
                54L);
        deleteTableRecordCount.put("DF_CS_1", 54);

        assertUpdateExpected(session, "CREATE TABLE DF_CS_2 as " +
                        "SELECT * " +
                        "FROM catalog_sales " +
                        "WHERE  cs_sold_date_sk >= (SELECT Min(d_date_sk) " +
                        "                           FROM   date_dim " +
                        "                           WHERE  d_date BETWEEN date'1999-09-18' AND date'1999-09-19') " +
                        "       AND cs_sold_date_sk <= (SELECT Max(d_date_sk) " +
                        "                               FROM   date_dim " +
                        "                               WHERE  d_date BETWEEN date'1999-09-18' AND " +
                        "                                                     date'1999-09-19') ",
                123L);
        deleteTableRecordCount.put("DF_CS_2", 123);

        assertUpdateExpected(session, "CREATE TABLE DF_CS_3 as " +
                        "SELECT * " +
                        "FROM catalog_sales " +
                        "WHERE  cs_sold_date_sk >= (SELECT Min(d_date_sk) " +
                        "                           FROM   date_dim " +
                        "                           WHERE  d_date BETWEEN date'2002-11-12' AND date'2002-11-13') " +
                        "       AND cs_sold_date_sk <= (SELECT Max(d_date_sk) " +
                        "                               FROM   date_dim " +
                        "                               WHERE  d_date BETWEEN date'2002-11-12' AND" +
                        "                                                     date'2002-11-13') ",
                197L);
        deleteTableRecordCount.put("DF_CS_3", 197);

        assertUpdateExpected(session, "CREATE TABLE DF_SR_1 as " +
                        "SELECT * " +
                        "FROM store_returns " +
                        "WHERE  sr_ticket_number IN (SELECT ss_ticket_number " +
                        "                            FROM   store_sales, " +
                        "                                   date_dim " +
                        "                            WHERE  ss_sold_date_sk = d_date_sk " +
                        "                                   AND d_date BETWEEN date'2000-05-20' AND " +
                        "                                                      date'2000-05-21') ",
                4L);
        deleteTableRecordCount.put("DF_SR_1", 4);

        assertUpdateExpected(session, "CREATE TABLE DF_SR_2 as " +
                        "SELECT * " +
                        "FROM store_returns " +
                        "WHERE  sr_ticket_number IN (SELECT ss_ticket_number " +
                        "                            FROM   store_sales, " +
                        "                                   date_dim " +
                        "                            WHERE  ss_sold_date_sk = d_date_sk " +
                        "                                   AND d_date BETWEEN date'1999-09-18' AND " +
                        "                                                      date'1999-09-19') ",
                13L);
        deleteTableRecordCount.put("DF_SR_2", 13);

        assertUpdateExpected(session, "CREATE TABLE DF_SR_3 as " +
                        "SELECT * " +
                        "FROM store_returns " +
                        "WHERE  sr_ticket_number IN (SELECT ss_ticket_number " +
                        "                            FROM   store_sales, " +
                        "                                   date_dim " +
                        "                            WHERE  ss_sold_date_sk = d_date_sk " +
                        "                                   AND d_date BETWEEN date'2002-11-12' AND " +
                        "                                                      date'2002-11-13') ",
                20L);
        deleteTableRecordCount.put("DF_SR_3", 20);

        assertUpdateExpected(session, "CREATE TABLE DF_SS_1 as " +
                        "SELECT * " +
                        "FROM store_sales " +
                        "WHERE  ss_sold_date_sk >= (SELECT Min(d_date_sk) " +
                        "                           FROM   date_dim " +
                        "                           WHERE  d_date BETWEEN date'2000-05-20' AND date'2000-05-21') " +
                        "       AND ss_sold_date_sk <= (SELECT Max(d_date_sk) " +
                        "                               FROM   date_dim " +
                        "                               WHERE  d_date BETWEEN date'2000-05-20' AND " +
                        "                                                     date'2000-05-21') ",
                64L);
        deleteTableRecordCount.put("DF_SS_1", 64);

        assertUpdateExpected(session, "CREATE TABLE DF_SS_2 as " +
                        "SELECT * " +
                        "FROM store_sales " +
                        "WHERE  ss_sold_date_sk >= (SELECT Min(d_date_sk) " +
                        "                           FROM   date_dim " +
                        "                           WHERE  d_date BETWEEN date'1999-09-18' AND date'1999-09-19') " +
                        "       AND ss_sold_date_sk <= (SELECT Max(d_date_sk) " +
                        "                               FROM   date_dim " +
                        "                               WHERE  d_date BETWEEN date'1999-09-18' AND " +
                        "                                                     date'1999-09-19') ",
                111L);
        deleteTableRecordCount.put("DF_SS_2", 111);

        assertUpdateExpected(session, "CREATE TABLE DF_SS_3 as " +
                        "SELECT * " +
                        "FROM store_sales " +
                        "WHERE  ss_sold_date_sk >= (SELECT Min(d_date_sk) " +
                        "                           FROM   date_dim " +
                        "                           WHERE  d_date BETWEEN date'2002-11-12' AND date'2002-11-13') " +
                        "       AND ss_sold_date_sk <= (SELECT Max(d_date_sk) " +
                        "                               FROM   date_dim " +
                        "                               WHERE  d_date BETWEEN date'2002-11-12' AND " +
                        "                                                     date'2002-11-13') ",
                185L);
        deleteTableRecordCount.put("DF_SS_3", 185);

        assertUpdateExpected(session, "CREATE TABLE DF_WR_1 as " +
                        "SELECT * " +
                        "FROM web_returns " +
                        "WHERE  wr_order_number IN (SELECT ws_order_number " +
                        "                           FROM   web_sales, " +
                        "                                  date_dim " +
                        "                           WHERE  ws_sold_date_sk = d_date_sk " +
                        "                                  AND d_date BETWEEN date'2000-05-20' AND " +
                        "                                                     date'2000-05-21') ",
                0L);
        deleteTableRecordCount.put("DF_WR_1", 0);

        assertUpdateExpected(session, "CREATE TABLE DF_WR_2 as " +
                        "SELECT * " +
                        "FROM web_returns " +
                        "WHERE  wr_order_number IN (SELECT ws_order_number " +
                        "                           FROM   web_sales, " +
                        "                                  date_dim " +
                        "                           WHERE  ws_sold_date_sk = d_date_sk " +
                        "                                  AND d_date BETWEEN date'1999-09-18' AND " +
                        "                                                     date'1999-09-19') ",
                0L);
        deleteTableRecordCount.put("DF_WR_2", 0);

        assertUpdateExpected(session, "CREATE TABLE DF_WR_3 as " +
                        "SELECT * " +
                        "FROM web_returns " +
                        "WHERE  wr_order_number IN (SELECT ws_order_number " +
                        "                           FROM   web_sales, " +
                        "                                  date_dim " +
                        "                           WHERE  ws_sold_date_sk = d_date_sk " +
                        "                                  AND d_date BETWEEN date'2002-11-12' AND " +
                        "                                                     date'2002-11-13') ",
                4L);
        deleteTableRecordCount.put("DF_WR_3", 4);

        assertUpdateExpected(session, "CREATE TABLE DF_WS_1 as " +
                        "SELECT * " +
                        "FROM web_sales " +
                        "WHERE  ws_sold_date_sk >= (SELECT Min(d_date_sk) " +
                        "                           FROM   date_dim " +
                        "                           WHERE  d_date BETWEEN date'2000-05-20' AND date'2000-05-21') " +
                        "       AND ws_sold_date_sk <= (SELECT Max(d_date_sk) " +
                        "                               FROM   date_dim " +
                        "                               WHERE  d_date BETWEEN date'2000-05-20' AND " +
                        "                                                     date'2000-05-21') ",
                0L);
        deleteTableRecordCount.put("DF_WS_1", 0);

        assertUpdateExpected(session, "CREATE TABLE DF_WS_2 as " +
                        "SELECT * " +
                        "FROM web_sales " +
                        "WHERE  ws_sold_date_sk >= (SELECT Min(d_date_sk) " +
                        "                           FROM   date_dim " +
                        "                           WHERE  d_date BETWEEN date'1999-09-18' AND date'1999-09-19') " +
                        "       AND ws_sold_date_sk <= (SELECT Max(d_date_sk) " +
                        "                               FROM   date_dim " +
                        "                               WHERE  d_date BETWEEN date'1999-09-18' AND " +
                        "                                                     date'1999-09-19') ",
                22L);
        deleteTableRecordCount.put("DF_WS_2", 22);

        assertUpdateExpected(session, "CREATE TABLE DF_WS_3 as " +
                        "SELECT * " +
                        "FROM web_sales " +
                        "WHERE  ws_sold_date_sk >= (SELECT Min(d_date_sk) " +
                        "                           FROM   date_dim " +
                        "                           WHERE  d_date BETWEEN date'2002-11-12' AND date'2002-11-13') " +
                        "       AND ws_sold_date_sk <= (SELECT Max(d_date_sk) " +
                        "                               FROM   date_dim " +
                        "                               WHERE  d_date BETWEEN date'2002-11-12' AND " +
                        "                                                     date'2002-11-13') ",
                42L);
        deleteTableRecordCount.put("DF_WS_3", 42);
    }

    @Test
    public void runDeletePhaseAndTest() throws Exception
    {
        for (Map.Entry<String, List<String>> kv : tableToDeleteTableMap.entrySet()) {
            for (String deleteTableName : kv.getValue()) {
                if (deleteTableRecordCount.get(deleteTableName) > 0) {
                    //addDeleteFiles(kv.getKey(), deleteTableName);
                    String schemaTableName = "tpcds" + "." + kv.getKey();
                    String deleteSchemaTableName = "tpcds" + "." + deleteTableName;
                    assertUpdateExpected(session,
                            format("CALL system.add_equality_deletes('%s','%s','%s')",
                                    schemaTableName,
                                    deleteSchemaTableName,
                                    deleteTableToColumnMap.get(deleteTableName)));
                }
            }
        }

        session = Session.builder(session)
                .setCatalogSessionProperty(ICEBERG_CATALOG,
                        "delete_as_join_rewrite_enabled",
                        "false")
                .build();
        runAllQueries();
    }

    private void runAllQueries() throws Exception
    {
        testTpcdsQ1();
        testTpcdsQ2();
        testTpcdsQ3();
        testTpcdsQ4();
        testTpcdsQ5();
        testTpcdsQ6();
        testTpcdsQ7();
        testTpcdsQ8();
        testTpcdsQ9();
        testTpcdsQ10();
        testTpcdsQ11();
        testTpcdsQ12();
        testTpcdsQ13();
        testTpcdsQ14_1();
        testTpcdsQ14_2();
        testTpcdsQ15();
        testTpcdsQ16();
        testTpcdsQ17();
        testTpcdsQ18();
        testTpcdsQ19();
        testTpcdsQ20();
        testTpcdsQ21();
        testTpcdsQ22();
        testTpcdsQ23_1();
        testTpcdsQ23_2();
        testTpcdsQ24_1();
        testTpcdsQ24_2();
        testTpcdsQ25();
        testTpcdsQ26();
        testTpcdsQ27();
        testTpcdsQ28();
        testTpcdsQ29();
        testTpcdsQ30();
        testTpcdsQ31();
        testTpcdsQ32();
        testTpcdsQ33();
        testTpcdsQ34();
        testTpcdsQ35();
        testTpcdsQ36();
        testTpcdsQ37();
        testTpcdsQ38();
        testTpcdsQ39_1();
        testTpcdsQ39_2();
        testTpcdsQ40();
        testTpcdsQ41();
        testTpcdsQ42();
        testTpcdsQ43();
        testTpcdsQ44();
        testTpcdsQ45();
        testTpcdsQ46();
        testTpcdsQ47();
        testTpcdsQ48();
        testTpcdsQ49();
        testTpcdsQ50();
        testTpcdsQ51();
        testTpcdsQ52();
        testTpcdsQ53();
        testTpcdsQ54();
        testTpcdsQ55();
        testTpcdsQ56();
        testTpcdsQ57();
        testTpcdsQ58();
        testTpcdsQ59();
        testTpcdsQ60();
        testTpcdsQ61();
        testTpcdsQ62();
        testTpcdsQ63();
        testTpcdsQ65();
        testTpcdsQ66();
        testTpcdsQ67();
        testTpcdsQ68();
        testTpcdsQ69();
        testTpcdsQ70();
        testTpcdsQ71();
        testTpcdsQ72();
        testTpcdsQ73();
        testTpcdsQ74();
        testTpcdsQ75();
        testTpcdsQ76();
        testTpcdsQ77();
        testTpcdsQ78();
        testTpcdsQ79();
        testTpcdsQ80();
        testTpcdsQ81();
        testTpcdsQ82();
        testTpcdsQ83();
        testTpcdsQ84();
        testTpcdsQ85();
        testTpcdsQ86();
        testTpcdsQ87();
        testTpcdsQ88();
        testTpcdsQ89();
        testTpcdsQ90();
        testTpcdsQ91();
        testTpcdsQ92();
        testTpcdsQ93();
        testTpcdsQ94();
        testTpcdsQ95();
        testTpcdsQ96();
        testTpcdsQ97();
        testTpcdsQ98();
        testTpcdsQ99();
    }

    private void addDeleteFiles(String tableName, String deleteTableName)
            throws IOException
    {
        String tableLocation = getLocation("catalog", tableName);
        System.out.println("Table location " + tableLocation);
        String deleteTableLocation = getLocation("catalog", deleteTableName);

        File[] files = ((new File(deleteTableLocation + "/data")).listFiles((dir, name) -> name.endsWith(".parquet")));
        assertTrue(files != null);

        CatalogManager catalogManager = getDistributedQueryRunner().getCoordinator().getCatalogManager();
        ConnectorId connectorId = catalogManager.getCatalog(ICEBERG_CATALOG).get().getConnectorId();
        Table icebergTable = getIcebergTable(getSession().toConnectorSession(connectorId), "tpcds", tableName);
        Transaction transaction = icebergTable.newTransaction();

        RowDelta rowDelta = transaction.newRowDelta();
        for (File deleteFilePath : files) {
            System.out.println("Delete File location " + deleteFilePath);

            File dest = new File(tableLocation + "/data/delete_file_" + deleteFilePath.getName());

            Files.copy(deleteFilePath.toPath(), dest.toPath());

            long rowCount = getParquetFileRowCount(dest);
            CommitTaskData task = new CommitTaskData(dest.getPath(),
                    dest.length(),
                    new MetricsWrapper(rowCount,
                            ImmutableMap.of(),
                            ImmutableMap.of(),
                            ImmutableMap.of(),
                            ImmutableMap.of(),
                            ImmutableMap.of(),
                            ImmutableMap.of()),
                    0,
                    Optional.empty(),
                    FileFormat.PARQUET, null);

            {
                PartitionSpec spec = icebergTable.specs().get(0);
                int fieldId = icebergTable.schema().findField(deleteTableToColumnMap.get(deleteTableName)).fieldId();

                FileMetadata.Builder builder = FileMetadata.deleteFileBuilder(spec)
                        .ofEqualityDeletes(fieldId)
                        .withPath(task.getPath())
                        .withFileSizeInBytes(task.getFileSizeInBytes())
                        .withFormat("parquet")
                        .withMetrics(task.getMetrics().metrics());

                if (!spec.fields().isEmpty()) {
                    String partitionDataJson = task.getPartitionDataJson()
                            .orElseThrow(() -> new VerifyException("No partition data for partitioned table"));
                    Type[] partitionColumnTypes = spec.fields().stream()
                            .map(field -> field.transform().getResultType(
                                    spec.schema().findType(field.sourceId())))
                            .toArray(Type[]::new);
                    builder.withPartition(PartitionData.fromJson(partitionDataJson, partitionColumnTypes));
                }
                rowDelta.addDeletes(builder.build());
            }
        }
        rowDelta.commit();
        transaction.commitTransaction();
    }

    protected String getLocation(String schema, String table)
    {
        File tempLocation = ((DistributedQueryRunner) getQueryRunner()).getCoordinator().getDataDirectory().toFile();
        return format("%s/%s/tpcds/%s", tempLocation.getPath(), schema, table);
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
    @Override
    public void tearDown()
    {
        for (String table : tpcdsTableNames) {
            assertUpdate(session, "DROP TABLE IF EXISTS " + table);
        }
    }
}
