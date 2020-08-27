package com.jiayi.kudu.demo;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.apache.kudu.shaded.com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author: jiayi
 * @description:
 * @date: 2020/8/26 2:54 PM
 * @version: v1.0.0
 */
@Slf4j
public class KuduJavaDemo {

    /**
     * kudu hosts
     */
    public static String masterAddresses = "192.168.13.188:7051,192.168.13.188:7151,192.168.13.188:7251";

    static KuduClient client;

    static {
        client = new KuduClient.KuduClientBuilder(masterAddresses).defaultAdminOperationTimeoutMs(60000).build();
        KuduSession session = client.newSession();
        // 此处所定义的是rpc链接超时
        session.setTimeoutMillis(60000);
    }

    @Test
    public void testCreateTable() {
        createTable("bigData");
    }

    @Test
    public void testDeleteTable() {
        deleteTable("bigData");
    }


    @Test
    public void testAlterTable() {
        String tableName = "bigData";
        try {
            Object o = 0L;
            // 建立非空的列
            client.alterTable(tableName, new AlterTableOptions().addColumn("device_id", Type.INT64, o));

            // 建立列为空
            client.alterTable(tableName, new AlterTableOptions().addNullableColumn("site_id", Type.INT64));
            // 删除字段
//            client.alterTable(tableName, new AlterTableOptions().dropColumn("site_id"));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testInsertData() {
        try {
            String tableName = "bigData";
            client = new KuduClient.KuduClientBuilder(masterAddresses).defaultAdminOperationTimeoutMs(60000).build();
            KuduSession session = client.newSession();
            // 此处所定义的是rpc链接超时
            session.setTimeoutMillis(60000);
            KuduTable table = client.openTable(tableName);
            /**
             * mode形式：
             * SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND 后台自动一次性批处理刷新提交N条数据
             * SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC  每次自动同步刷新提交每条数据
             * SessionConfiguration.FlushMode.MANUAL_FLUSH     手动刷新一次性提交N条数据
             */
            session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH); //mode形式
            session.setMutationBufferSpace(10000);// 缓冲大小，也就是数据的条数

            // 插入时，初始时间
            long startTime = System.currentTimeMillis();

            int val = 0;
            // 插入数据
            for (int i = 0; i < 60; i++) {
                Insert insert = table.newInsert();
                PartialRow row = insert.getRow();
                // row.addString("字段名", 字段值)、row.addLong(第几列, 字段值)
                row.addLong(0, i); //指第一个字段 "id"（hash分区的联合主键之一）
                row.addLong(1, i * 100);//指第二个字段 "user_id"（hash分区的联合主键之一）
                row.addLong(2, i);//指第三个字段 "start_time"（range分区字段）
                row.addString(3, "bigData");//指第四个字段 "name"
                session.apply(insert);
                if (val % 10 == 0) {
                    session.flush(); //手动刷新提交
                    val = 0;
                }
                val++;
            }
            session.flush(); //手动刷新提交
            // 插入时结束时间
            long endTime = System.currentTimeMillis();
            System.out.println("the timePeriod executed is : " + (endTime - startTime) + "ms");
            session.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void selectData(){
        try {
            String tableName = "bigData";


            // 获取须要查询数据的列
            List<String> projectColumns = new ArrayList<String>();
            projectColumns.add("id");
            projectColumns.add("user_id");
            projectColumns.add("start_time");
            projectColumns.add("name");

            KuduTable table = client.openTable(tableName);

            // 简单的读取
            KuduScanner scanner = client.newScannerBuilder(table).setProjectedColumnNames(projectColumns).build();


            while (scanner.hasMoreRows()) {
                RowResultIterator results = scanner.nextRows();
                // 15个tablet，每次从tablet中获取的数据的行数
                int numRows = results.getNumRows();
                System.out.println("numRows count is : " + numRows);
                while (results.hasNext()) {
                    RowResult result = results.next();
                    long id = result.getLong(0);
                    long user_id = result.getLong(1);
                    long start_time = result.getLong(2);
                    String name = result.getString(3);
                    System.out.println("id is : " + id + "  ===  user_id is : " + user_id + "  ===  start_time : " + start_time + "  ===  name is : " + name);
                }
                System.out.println("--------------------------------------");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void upsertData(){
        try {
            String tableName = "bigData";

            // 获取table
            KuduTable table = client.openTable(tableName);
            /**
             * mode形式：
             * SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND 后台自动一次性批处理刷新提交N条数据
             * SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC  每次自动同步刷新提交每条数据
             * SessionConfiguration.FlushMode.MANUAL_FLUSH     手动刷新一次性提交N条数据
             */
            // 获取一个会话
            KuduSession session = client.newSession();
            session.setTimeoutMillis(60000);
            session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH); //手动刷新一次性提交N条数据
            session.setMutationBufferSpace(10000); // 缓冲大小，也就是数据的条数

            // 插入时，初始时间
            long startTime = System.currentTimeMillis();

            int val = 0;
            // 在使用 upsert 语句时，当前须要 三个条件（key）都知足的状况下，才能够更新数据，不然就是插入数据
            // 三个条件（key） 分别指的是 hash分区的联合主键id、user_id，还有range分区字段 start_time
            for (int i = 0; i < 60; i++) {
                //upsert into 表名 values (‘xx’,123) 若是指定的values中的主键值 在表中已经存在，则执行update语义，反之，执行insert语义。
                Upsert upsert = table.newUpsert();
                PartialRow row = upsert.getRow();
                row.addLong(0, i); //指第一个字段 "id"（hash分区的联合主键之一）
                row.addLong(1, i*100); //指第二个字段 "user_id"（hash分区的联合主键之一）
                row.addLong(2, i); //指第三个字段 "start_time"（range分区字段）
                row.addString(3, "bigData"+i); //指第四个字段 "name"
                session.apply(upsert);
                if (val % 10 == 0) {
                    session.flush(); //手动刷新提交
                    val = 0;
                }
                val++;
            }
            session.flush(); //手动刷新提交
            // 插入时结束时间
            long endTime = System.currentTimeMillis();
            System.out.println("the timePeriod executed is : " + (endTime - startTime) + "ms");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 删除表
     */
    private static void deleteTable(String tableName) {
        try {
            // 测试，若是table存在的状况下，就删除该表
            if (client.tableExists(tableName)) {
                client.deleteTable(tableName);
                System.out.println("delete the table！");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                client.shutdown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 创建表
     *
     * @param tableName
     */
    private static void createTable(String tableName) {
        try {


            List<ColumnSchema> columns = Lists.newArrayList();

            // 建立列
            columns.add(new ColumnSchema.ColumnSchemaBuilder("id", Type.INT64).key(true).build());
            columns.add(new ColumnSchema.ColumnSchemaBuilder("user_id", Type.INT64).key(true).build());
            columns.add(new ColumnSchema.ColumnSchemaBuilder("start_time", Type.INT64).key(true).build());
            columns.add(new ColumnSchema.ColumnSchemaBuilder("name", Type.STRING).nullable(true).build());

            // 建立schema
            Schema schema = new Schema(columns);

            /*
               建立 hash分区 + range分区 二者同时使用 的表
                addHashPartitions(ImmutableList.of("字段名1","字段名2",...), hash分区数量)  默认使用主键，也可另外指定联合主键
                setRangePartitionColumns(ImmutableList.of("字段名"))
            */
            // id，user_id至关于联合主键，三个条件都知足的状况下，才能够更新数据，不然就是插入数据
            ImmutableList<String> hashKeys = ImmutableList.of("id", "user_id");
            CreateTableOptions tableOptions = new CreateTableOptions();
           /*
            建立 hash分区 + range分区 二者同时使用 的表
            addHashPartitions(ImmutableList.of("字段名1","字段名2",...), hash分区数量)  默认使用主键，也可另外指定联合主键
            setRangePartitionColumns(ImmutableList.of("字段名"))
           */
            // 设置hash分区，包括分区数量、副本数目
            tableOptions.addHashPartitions(hashKeys, 3); //hash分区数量
            tableOptions.setNumReplicas(3); //副本数目

            // 设置range分区
            tableOptions.setRangePartitionColumns(ImmutableList.of("start_time"));

            // 设置range分区数量
            // 规则：range范围为时间戳是1-10，10-20，20-30，30-40，40-50
            int count = 0;
            for (long i = 1; i < 6; i++) {
                PartialRow lower = schema.newPartialRow();
                lower.addLong("start_time", count);
                PartialRow upper = schema.newPartialRow();
                count += 10;
                upper.addLong("start_time", count);
                tableOptions.addRangePartition(lower, upper);
            }

            // 建立table,并设置partition
            client.createTable(tableName, schema, tableOptions);
            System.out.println("create table is success!");

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                client.shutdown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
