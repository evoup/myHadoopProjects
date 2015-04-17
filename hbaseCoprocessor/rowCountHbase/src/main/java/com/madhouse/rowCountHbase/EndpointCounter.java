package com.madhouse.rowCountHbase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;

public class EndpointCounter {
    public static Map <Object,Long> map;

    public static void main(String[] args) throws IOException {
        map = new HashMap<Object,Long> (); 
        Configuration conf = HBaseConfiguration.create();
        HBaseHelper helper = HBaseHelper.getHelper(conf);
        System.out.println("Before endpoint call...");
        HTable table = new HTable(conf, "test15");
        try {
            table.coprocessorExec(
                    RowCountProtocol.class, // ClassName Define the protocol interface being invoked.
                    null, null, // Set start and end row key to "null" to count all rows.
                    new Batch.Call<RowCountProtocol, Long>() { // 创建匿名类发送到全部region server
                        @Override
                public Long call(RowCountProtocol counter) throws IOException {
                    Long val_aid=counter.getRowCount(new SingleColumnValueFilter(Bytes.toBytes("info"),Bytes.toBytes("aid"),CompareOp.EQUAL,Bytes.toBytes("1")));
                    addItem("aid",val_aid);
                    Long val_did=counter.getRowCount(new SingleColumnValueFilter(Bytes.toBytes("info"),Bytes.toBytes("did"),CompareOp.EQUAL,Bytes.toBytes("1")));
                    addItem("did",val_did);
                    Long val_oid=counter.getRowCount(new SingleColumnValueFilter(Bytes.toBytes("info"),Bytes.toBytes("oid"),CompareOp.EQUAL,Bytes.toBytes("1")));
                    addItem("oid",val_oid);
                    Long val_uid=counter.getRowCount(new SingleColumnValueFilter(Bytes.toBytes("info"),Bytes.toBytes("uid"),CompareOp.EQUAL,Bytes.toBytes("1")));
                    addItem("uid",val_uid);
                    Long val_vid=counter.getRowCount(new SingleColumnValueFilter(Bytes.toBytes("info"),Bytes.toBytes("vid"),CompareOp.EQUAL,Bytes.toBytes("1")));
                    addItem("vid",val_vid);
                    Long val_model=counter.getRowCount(new SingleColumnValueFilter(Bytes.toBytes("info"),Bytes.toBytes("model"),CompareOp.EQUAL,Bytes.toBytes("1")));
                    addItem("model",val_model);
                    Long val_useragent=counter.getRowCount(new SingleColumnValueFilter(Bytes.toBytes("info"),Bytes.toBytes("useragent"),CompareOp.EQUAL,Bytes.toBytes("1")));
                    addItem("useragent",val_useragent);
                    Long val_tma=counter.getRowCount(new SingleColumnValueFilter(Bytes.toBytes("info"),Bytes.toBytes("tma"),CompareOp.EQUAL,Bytes.toBytes("1")));
                    addItem("tma",val_tma);
                    Long val_wma=counter.getRowCount(new SingleColumnValueFilter(Bytes.toBytes("info"),Bytes.toBytes("wma"),CompareOp.EQUAL,Bytes.toBytes("1")));
                    addItem("wma",val_wma);
                    Long val_idfa=counter.getRowCount(new SingleColumnValueFilter(Bytes.toBytes("info"),Bytes.toBytes("idfa"),CompareOp.EQUAL,Bytes.toBytes("1")));
                    addItem("idfa",val_idfa);
                    //return counter.getRowCount(new SingleColumnValueFilter(Bytes.toBytes("info"),Bytes.toBytes("wma"),CompareOp.EQUAL,Bytes.toBytes("1")));
                    return new Long(1); //随便返回一个数据，不是使用返回的方式 
                }
                });

            System.out.println("res");
            for ( Map.Entry<Object, Long> entry : map.entrySet() ) { 
                System.out.println(entry.getKey()+">>>>>>>>>>>>>"+entry.getValue());
            }
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }

    public static void addItem(String key,Long val) {
        if(map.containsKey(key)) {
            map.put(key,map.get(key)+val);
        } else {
            map.put(key,val);
        }
    }
}
