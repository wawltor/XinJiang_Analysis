package com.analysis.solr.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;


public class solrIndexer {

    /**
     * @param args
     * @throws IOException
     * @throws SolrServerException
     */
    @SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException,
            SolrServerException {
        final Configuration conf;
        SolrServer solrServer = new HttpSolrServer("http://10.105.245.185:8983/solr/collection1");
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "10.105.245.166,10.105.245.211");
        HTable table = new HTable(conf, "record"); // 这里指定HBase表名称
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("d")); // 这里指定HBase表的列族
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        ResultScanner ss = table.getScanner(scan);

        System.out.println("start ...");
        int i = 0;
        try {
            for (Result r : ss) {
                SolrInputDocument solrDoc = new SolrInputDocument();
                solrDoc.addField("rowkey", new String(r.getRow()));
                for (KeyValue kv : r.raw()) {
                    String fieldName = new String(kv.getQualifier());
                    String fieldValue = new String(kv.getValue());
                    if ( fieldName.equalsIgnoreCase("Name")
                            || fieldName.equalsIgnoreCase("Sex")
                            || fieldName.equalsIgnoreCase("Nation")
                            || fieldName.equalsIgnoreCase("Birthday")
                            || fieldName.equalsIgnoreCase("Address")
                            || fieldName.equalsIgnoreCase("CardNO")
                            || fieldName.equalsIgnoreCase("Dubious")
                            ||fieldName.equalsIgnoreCase("EntryTime")
                    		) {
                        solrDoc.addField(fieldName, fieldValue);
                    }
                }
                solrServer.add(solrDoc);
                solrServer.commit(true, true, true);
                i = i + 1;
                System.out.println("已经成功处理 " + i + " 条数据");
            }
            ss.close();
            table.close();
            System.out.println("done !");
        } catch (IOException e) {
        } finally {
            ss.close();
            table.close();
            System.out.println("erro !");
        }
    }

}