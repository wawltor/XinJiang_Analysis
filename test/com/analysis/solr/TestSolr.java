package com.analysis.solr;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.junit.Test;

public class TestSolr {

	@Test
	public void test() throws IOException, SolrServerException {
		final Configuration conf;
        conf = HBaseConfiguration.create();
        HTable table = new HTable(conf, "record");
        Get get = null;
        List<Get> list = new ArrayList<Get>();
        
        String url = "http://localhost:8983/solr";
        SolrServer server = new HttpSolrServer(url);
        SolrQuery query = new SolrQuery("Sex:男");
        query.setStart(0); //数据起始行，分页用
        query.setRows(10); //返回记录数，分页用
        QueryResponse response = server.query(query);
        SolrDocumentList docs = response.getResults();
        System.out.println("文档个数：" + docs.getNumFound()); //数据总条数也可轻易获取
        System.out.println("查询时间：" + response.getQTime()); 
        for (SolrDocument doc : docs) {
            get = new Get(Bytes.toBytes((String) doc.getFieldValue("rowkey")));
            list.add(get);
        }
        
        Result[] res = table.get(list);
        
        byte[] bt1 = null;
        byte[] bt2 = null;
        byte[] bt3 = null;
        byte[] bt4 = null;
        String str1 = null;
        String str2 = null;
        String str3 = null;
        String str4 = null;
        for (Result rs : res) {
            bt1 = rs.getValue("d".getBytes(), "Nation".getBytes());
            bt2 = rs.getValue("d".getBytes(), "Sex".getBytes());
            bt3 = rs.getValue("d".getBytes(), "Address".getBytes());
            bt4 = rs.getValue("d".getBytes(), "CardNO".getBytes());
            if (bt1 != null && bt1.length>0) {str1 = new String(bt1);} else {str1 = "无数据";} //对空值进行new String的话会抛出异常
            if (bt2 != null && bt2.length>0) {str2 = new String(bt2);} else {str2 = "无数据";}
            if (bt3 != null && bt3.length>0) {str3 = new String(bt3);} else {str3 = "无数据";}
            if (bt4 != null && bt4.length>0) {str4 = new String(bt4);} else {str4 = "无数据";}
            System.out.print(new String(rs.getRow()) + " ");
            System.out.print(str1 + "|");
            System.out.print(str2 + "|");
            System.out.print(str3 + "|");
            System.out.println(str4 + "|");
        }
        table.close();
    }
	}


