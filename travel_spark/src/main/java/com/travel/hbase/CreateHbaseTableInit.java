package com.travel.hbase;

import com.travel.common.Constants;
import com.travel.utils.HbaseTools;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author wangyuxuan
 * @date 2020/3/11 13:41
 * @description 创建HBase表（预分区）
 */
public class CreateHbaseTableInit {
    public static void main(String[] args) throws IOException {
        Connection hbaseConn = HbaseTools.getHbaseConn();
        String[] tableNames = {"order_info", "renter_info", "driver_info", "opt_alliance_business"};
        CreateHbaseTableInit createHbaseTableInit = new CreateHbaseTableInit();
        for (String tableName : tableNames) {
            createHbaseTableInit.createTable(hbaseConn, Constants.DEFAULT_REGION_NUM, tableName);
        }
        hbaseConn.close();
    }

    public void createTable(Connection connection, int regionNum, String tableName) throws IOException {
        Admin admin = connection.getAdmin();
        if (admin.tableExists(TableName.valueOf(tableName))) {
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
        }

        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(Constants.DEFAULT_DB_FAMILY);
        hTableDescriptor.addFamily(hColumnDescriptor);
        byte[][] splitKey = getSplitKey(regionNum);
        admin.createTable(hTableDescriptor, splitKey);
        admin.close();
    }

    public byte[][] getSplitKey(int regionNum) {
        // 创建二进制的字节数组，用于分区
        byte[][] byteNum = new byte[regionNum][];
        for (int i = 0; i < regionNum; i++) {
            String leftPad = StringUtils.leftPad(i + "", 4, "0");
            byteNum[i] = Bytes.toBytes(leftPad + "|");
        }
        return byteNum;
    }
}
