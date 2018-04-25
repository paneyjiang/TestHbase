package com.sitech.hbase;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;

public class HbaseConnection {
	  private static HConnection connection;
	  private  static Configuration configuration=null;
	static {
        try {
        	configuration= HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.property.clientPort", "2181");
            configuration.set("hbase.zookeeper.quorum","192.168.1.108");
    	//	configuration.set("hbase.master", "172.21.10.137:60010");
            ExecutorService pool = new ThreadPoolExecutor(0, 5000, 60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), new ThreadPoolExecutor.DiscardOldestPolicy());
            //初始化连接池A
            connection = HConnectionManager.createConnection(configuration, pool);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
	public static HConnection getConnection(){
		return connection;
	}
	public static void createTable(String tablename,String column){
		try {
			HBaseAdmin admin = new HBaseAdmin(configuration);
			//creating table descriptor
			HTableDescriptor table = new HTableDescriptor(tablename);
			//creating column family descriptor
			HColumnDescriptor family = new HColumnDescriptor(column);
			table.addFamily(family);

		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
