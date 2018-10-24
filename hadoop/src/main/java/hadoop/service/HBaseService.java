package hadoop.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.RowMapper;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author alan.huang
 */
@Service
public class HBaseService {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private Configuration configuration;

    @Autowired
    private HbaseTemplate hbaseTemplate;


    public void createTable(String tableName, String... families) {
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        try (Connection connection = ConnectionFactory.createConnection(configuration);
             Admin admin = connection.getAdmin()) {
            for (String family : families) {
                tableDescriptor.addFamily(new HColumnDescriptor(family));
            }
            if (admin.tableExists(TableName.valueOf(tableName))) {
                System.out.println("Table Exists");
                logger.info("Table:[" + tableName + "] Exists");
            } else {
                admin.createTable(tableDescriptor);
                System.out.println("Create table Successfully!!!Table Name:[" + tableName + "]");
                logger.info("Create table Successfully!!!Table Name:[" + tableName + "]");
            }
        } catch (IOException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }
    }


    public void putRowValue(String tableName, String rowKey, String familyColumn, String columnName, String value) {
        try (Connection connection = ConnectionFactory.createConnection(configuration);
             Table table = connection.getTable(TableName.valueOf(tableName))) {
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(familyColumn), Bytes.toBytes(columnName), Bytes.toBytes(value));
            table.put(put);
            logger.info("update table:" + tableName + ",rowKey:" + rowKey + ",family:" + familyColumn + ",column:" + columnName + ",value:" + value + " successfully!");
            System.out.println("Update table success");
        } catch (IOException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }
    }


    public List<Cell> scanRegexRowKey(String tableName, String regexKey) {
        try (Connection connection = ConnectionFactory.createConnection(configuration);
             Table table = connection.getTable(TableName.valueOf(tableName))) {
            Scan scan = new Scan();
            Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(regexKey));
            scan.setFilter(filter);
            ResultScanner rs = table.getScanner(scan);
            for (Result r : rs) {
                return r.listCells();
            }
        } catch (IOException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }
        return null;
    }


    public void aa() {
        TableName tableName = TableName.valueOf("demo");


        //put
        String rowKey = "u12000";

        hbaseTemplate.put("ota_pre_record", rowKey, "info", "name", Bytes.toBytes("aaa"));
        hbaseTemplate.find("ota_pre_record", "info", (result, i) -> {
            List<Cell> ceList = result.listCells();
            Map<String, Object> map = new HashMap<>(16);
            if (ceList != null && ceList.size() > 0) {
                for (Cell cell : ceList) {
                    map.put(Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()) +
                                    "_" + Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()),
                            Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
            }
            map.forEach((s, o) -> {
                System.out.println("############"+o);
            });
            return map;

        });
    }

}
