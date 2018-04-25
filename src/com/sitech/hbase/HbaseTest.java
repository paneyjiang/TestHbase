package com.sitech.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseTest {

	public static void main(String[] args) throws IOException {
		HConnection con=HbaseConnection.getConnection();
		System.out.println(con);
		
	//	HbaseConnection.createTable("ia_login_opr","c");
		//con.
	//	HTableInterface table = con.getTable("testtable");
		HTable messages = (HTable) con.getTable("testable");
		TableName name =messages.getName();
		System.out.println("====="+name);
		Get get = new Get(Bytes.toBytes("rowkey001"));
		//Put put = new Put();
		//message
		Result  r = messages.get(get);
		//根据filter 查询scan
		/*Filter filter = new ValueFilter(CompareOp.EQUAL, new SubstringComparator("123456"));  
		Scan  scan = new Scan();
		scan.setFilter(filter);*/
		//messages.put(arg0);
		System.out.println("---"+r);
		List<Cell> resultList = r.listCells();
		 for(Cell cell :resultList){
			String value =  Bytes.toString( CellUtil.cloneValue(cell));
		 }
		/*for(Cell cell :r.listCells()){
			System.out.println("qualifier:" +Bytes.toString(CellUtil.cloneQualifier(cell)) );  
			 System.out.println("value:" +Bytes.toString(CellUtil.cloneValue(cell)) );  
			  System.out.println("-------------------------------");  

		}
		messages.close();*/
	}
}
