package com.analysis.solr.hbase;
import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
public class SolrHBaseIndexer {
	private static void usage() {
        System.err.println("输入参数: <配置文件路径> <起始行> <结束行>");
        System.exit(1);
    }

    private static Configuration conf;

    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException, URISyntaxException {

        if (args.length == 0 || args.length > 1) {
            usage();
        }

        createHBaseConfiguration(args[0]);
        ConfigProperties tutorialProperties = new ConfigProperties(args[0]);
        String tbName = tutorialProperties.getHBTbName();
        String tbFamily = tutorialProperties.getHBFamily();
        conf.set("job.mapred.tracker","10.105.245.185:9001");
        @SuppressWarnings("deprecation")
		Job job = new Job(conf, "SolrHBaseIndexer");
        job.setJarByClass(SolrHBaseIndexer.class);

        Scan scan = new Scan();
        /*if (args.length == 3) {
            scan.setStartRow(Bytes.toBytes(args[1]));
            scan.setStopRow(Bytes.toBytes(args[2]));
        }*/

        scan.addFamily(Bytes.toBytes(tbFamily));
        scan.setCaching(500); // 设置缓存数据量来提高效率
        scan.setCacheBlocks(false);

        // 创建Map任务
        TableMapReduceUtil.initTableMapperJob(tbName, scan,
                SolrIndexHbaseMapper.class, null, null, job);

        // 不需要输出
        job.setOutputFormatClass(NullOutputFormat.class);
        // job.setNumReduceTasks(0);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * 从配置文件读取并设置HBase配置信息
     * 
     * @param propsLocation
     * @return
     */
    private static void createHBaseConfiguration(String propsLocation) {
        ConfigProperties tutorialProperties = new ConfigProperties(
                propsLocation);
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", tutorialProperties.getZKQuorum());
        conf.set("hbase.zookeeper.property.clientPort",
                tutorialProperties.getZKPort());
        conf.set("hbase.master", tutorialProperties.getHBMaster());
        conf.set("hbase.rootdir", tutorialProperties.getHBrootDir());
        conf.set("solr.server", tutorialProperties.getSolrServer());
    }

}
