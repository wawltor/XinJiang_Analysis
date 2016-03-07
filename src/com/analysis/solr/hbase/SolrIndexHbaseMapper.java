package com.analysis.solr.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

public class SolrIndexHbaseMapper extends TableMapper<Text, Text> {

    @SuppressWarnings("deprecation")
	public void map(ImmutableBytesWritable key, Result hbaseResult,
            Context context) throws InterruptedException, IOException {

        Configuration conf = context.getConfiguration();
       // conf.set("solr.sever","http://localhost:8983/solr");
        HttpSolrServer solrServer = new HttpSolrServer(conf.get("solr.server"));
        solrServer.setDefaultMaxConnectionsPerHost(100);
        solrServer.setMaxTotalConnections(1000);
        solrServer.setSoTimeout(20000);
        solrServer.setConnectionTimeout(20000);
        SolrInputDocument solrDoc = new SolrInputDocument();
        try {
            solrDoc.addField("rowkey", new String(hbaseResult.getRow()));
            for (KeyValue rowQualifierAndValue : hbaseResult.list()) {
        
				String fieldName = new String(
                        rowQualifierAndValue.getQualifier());
               
				String fieldValue = new String(rowQualifierAndValue.getValue());
                if ( fieldName.equalsIgnoreCase("Name")
                        || fieldName.equalsIgnoreCase("Sex")
                        || fieldName.equalsIgnoreCase("CardNO")
                        || fieldName.equalsIgnoreCase("EntryPosition")
                        || fieldName.equalsIgnoreCase("Dubious")
                		) {
                    solrDoc.addField(fieldName, fieldValue);
                }
            }
            solrServer.add(solrDoc);
            solrServer.commit(true, true, true);
        } catch (SolrServerException e) {
            System.err.println("更新Solr索引异常:" + new String(hbaseResult.getRow()));
        }
    }
}