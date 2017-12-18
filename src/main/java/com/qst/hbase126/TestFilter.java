package com.qst.hbase126;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.DependentColumnFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class TestFilter {
	
	/**
	 * 行过滤器&二进制比较器
	 * @throws Exception
	 */
	@Test
	public void rowFilter() throws Exception {
		// 获取hbase配置对象
		Configuration conf = HBaseConfiguration.create();
		// 通过对象创建连接
		Connection conn = ConnectionFactory.createConnection(conf);
		// 得到table对象
		HTable table = (HTable) conn.getTable(TableName.valueOf("ns1:t3"));
		// 获取scan对象
		Scan scan = new Scan();
		
		Filter filter = new RowFilter(CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes("row5")));
		scan.setFilter(filter);
		
		// 获取扫描器
		ResultScanner scanner = table.getScanner(scan);
		// 通过扫描器获得结果集
		Iterator<Result> it = scanner.iterator();
		while(it.hasNext()){
			Result rs = it.next();
			toStr(rs,"f1","name");
			toStr(rs,"f1","id");
			toStr(rs,"f1","age");
			toStr(rs,"f2","id");
			toStr(rs,"f2","age");
			toStr(rs,"f2","name");
			System.out.println("==============================");
		}
		//System.out.println(System.currentTimeMillis() - start);

	}
	/**
	 * 行过滤器&正则比较器
	 * @throws Exception
	 */
	@Test
	public void rowFilter2() throws Exception {
		// 获取hbase配置对象
		Configuration conf = HBaseConfiguration.create();
		// 通过对象创建连接
		Connection conn = ConnectionFactory.createConnection(conf);
		// 得到table对象
		HTable table = (HTable) conn.getTable(TableName.valueOf("ns1:t3"));
		// 获取scan对象
		Scan scan = new Scan();
		
		Filter filter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator(".*[5]"));
		scan.setFilter(filter);
		
		// 获取扫描器
		ResultScanner scanner = table.getScanner(scan);
		// 通过扫描器获得结果集
		Iterator<Result> it = scanner.iterator();
		while(it.hasNext()){
			Result rs = it.next();
			toStr(rs,"f1","name");
			toStr(rs,"f1","id");
			toStr(rs,"f1","age");
			toStr(rs,"f2","id");
			toStr(rs,"f2","age");
			toStr(rs,"f2","name");
			System.out.println("==============================");
		}
		//System.out.println(System.currentTimeMillis() - start);

	}
	/**
	 * 行过滤器&子串比较器
	 * @throws Exception
	 */
	@Test
	public void rowFilter3() throws Exception {
		// 获取hbase配置对象
		Configuration conf = HBaseConfiguration.create();
		// 通过对象创建连接
		Connection conn = ConnectionFactory.createConnection(conf);
		// 得到table对象
		HTable table = (HTable) conn.getTable(TableName.valueOf("ns1:t3"));
		// 获取scan对象
		Scan scan = new Scan();
		
		Filter filter = new RowFilter(CompareOp.EQUAL, new SubstringComparator("w5"));
		scan.setFilter(filter);
		
		// 获取扫描器
		ResultScanner scanner = table.getScanner(scan);
		// 通过扫描器获得结果集
		Iterator<Result> it = scanner.iterator();
		while(it.hasNext()){
			Result rs = it.next();
			toStr(rs,"f1","name");
			toStr(rs,"f1","id");
			toStr(rs,"f1","age");
			toStr(rs,"f2","id");
			toStr(rs,"f2","age");
			toStr(rs,"f2","name");
			System.out.println("==============================");
		}
		//System.out.println(System.currentTimeMillis() - start);

	}
	
	/**
	 * 列族过滤器&二进制比较器
	 * @throws Exception
	 */
	@Test
	public void cfFilter() throws Exception {
		// 获取hbase配置对象
		Configuration conf = HBaseConfiguration.create();
		// 通过对象创建连接
		Connection conn = ConnectionFactory.createConnection(conf);
		// 得到table对象
		HTable table = (HTable) conn.getTable(TableName.valueOf("ns1:t3"));
		// 获取scan对象
		Scan scan = new Scan();
		
		Filter filter = new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("f1")));
		scan.setFilter(filter);
		
		// 获取扫描器
		ResultScanner scanner = table.getScanner(scan);
		// 通过扫描器获得结果集
		Iterator<Result> it = scanner.iterator();
		while(it.hasNext()){
			Result rs = it.next();
			toStr(rs,"f1","name");
			toStr(rs,"f1","id");
			toStr(rs,"f1","age");
			toStr(rs,"f2","id");
			toStr(rs,"f2","age");
			toStr(rs,"f2","name");
			System.out.println("==============================");
		}
		//System.out.println(System.currentTimeMillis() - start);
	}
	/**
	 * 列名过滤器&二进制比较器
	 * @throws Exception
	 */
	@Test
	public void colFilter() throws Exception {
		// 获取hbase配置对象
		Configuration conf = HBaseConfiguration.create();
		// 通过对象创建连接
		Connection conn = ConnectionFactory.createConnection(conf);
		// 得到table对象
		HTable table = (HTable) conn.getTable(TableName.valueOf("ns1:t3"));
		// 获取scan对象
		Scan scan = new Scan();
		
		Filter filter = new QualifierFilter(CompareOp.NOT_EQUAL, new BinaryComparator(Bytes.toBytes("name")));
		scan.setFilter(filter);
		
		// 获取扫描器
		ResultScanner scanner = table.getScanner(scan);
		// 通过扫描器获得结果集
		Iterator<Result> it = scanner.iterator();
		while(it.hasNext()){
			Result rs = it.next();
			toStr(rs,"f1","name");
			toStr(rs,"f1","id");
			toStr(rs,"f1","age");
			toStr(rs,"f2","id");
			toStr(rs,"f2","age");
			toStr(rs,"f2","name");
			System.out.println("==============================");
		}
		//System.out.println(System.currentTimeMillis() - start);
	}
	
	/**
	 * 值过滤器&二进制比较器
	 * @throws Exception
	 */
	@Test
	public void valFilter() throws Exception {
		// 获取hbase配置对象
		Configuration conf = HBaseConfiguration.create();
		// 通过对象创建连接
		Connection conn = ConnectionFactory.createConnection(conf);
		// 得到table对象
		HTable table = (HTable) conn.getTable(TableName.valueOf("ns1:t3"));
		// 获取scan对象
		Scan scan = new Scan();
		
		Filter filter = new ValueFilter(CompareOp.GREATER_OR_EQUAL, new BinaryComparator(Bytes.toBytes("20")));
		scan.setFilter(filter);
		
		// 获取扫描器
		ResultScanner scanner = table.getScanner(scan);
		// 通过扫描器获得结果集
		Iterator<Result> it = scanner.iterator();
		while(it.hasNext()){
			Result rs = it.next();
			toStr(rs,"f1","name");
			toStr(rs,"f1","id");
			toStr(rs,"f1","age");
			toStr(rs,"f2","id");
			toStr(rs,"f2","age");
			toStr(rs,"f2","name");
			System.out.println("==============================");
		}
		//System.out.println(System.currentTimeMillis() - start);
	}
	/**
	 * 列依赖过滤器
	 * @throws Exception
	 */
	@Test
	public void depFilter() throws Exception {
		// 获取hbase配置对象
		Configuration conf = HBaseConfiguration.create();
		// 通过对象创建连接
		Connection conn = ConnectionFactory.createConnection(conf);
		// 得到table对象
		HTable table = (HTable) conn.getTable(TableName.valueOf("ns1:t3"));
		// 获取scan对象
		Scan scan = new Scan();
		
		Filter filter = new DependentColumnFilter(
				Bytes.toBytes("f2"), Bytes.toBytes("name"), true);
		scan.setFilter(filter);
		
		// 获取扫描器
		ResultScanner scanner = table.getScanner(scan);
		// 通过扫描器获得结果集
		Iterator<Result> it = scanner.iterator();
		while(it.hasNext()){
			Result rs = it.next();
			toStr(rs,"f1","name");
			toStr(rs,"f1","id");
			toStr(rs,"f1","age");
			toStr(rs,"f2","id");
			toStr(rs,"f2","age");
			toStr(rs,"f2","name");
			System.out.println("==============================");
		}
		//System.out.println(System.currentTimeMillis() - start);
	}
	
	/**
	 * 单列值过滤器
	 * @throws Exception
	 */
	@Test
	public void singleColFilter() throws Exception {
		// 获取hbase配置对象
		Configuration conf = HBaseConfiguration.create();
		// 通过对象创建连接
		Connection conn = ConnectionFactory.createConnection(conf);
		// 得到table对象
		HTable table = (HTable) conn.getTable(TableName.valueOf("ns1:t3"));
		// 获取scan对象
		Scan scan = new Scan();
		
		Filter filter = new SingleColumnValueExcludeFilter(Bytes.toBytes("f1"), 
				Bytes.toBytes("name"), CompareOp.GREATER_OR_EQUAL, Bytes.toBytes("tom20"));
		scan.setFilter(filter);
		
		// 获取扫描器
		ResultScanner scanner = table.getScanner(scan);
		// 通过扫描器获得结果集
		Iterator<Result> it = scanner.iterator();
		while(it.hasNext()){
			Result rs = it.next();
			toStr(rs,"f1","name");
			toStr(rs,"f1","id");
			toStr(rs,"f1","age");
			toStr(rs,"f2","id");
			toStr(rs,"f2","age");
			toStr(rs,"f2","name");
			System.out.println("==============================");
		}
		//System.out.println(System.currentTimeMillis() - start);
	}
	
	/**
	 * 前缀过滤器
	 * @throws Exception
	 */
	@Test
	public void prefixFilter() throws Exception {
		// 获取hbase配置对象
		Configuration conf = HBaseConfiguration.create();
		// 通过对象创建连接
		Connection conn = ConnectionFactory.createConnection(conf);
		// 得到table对象
		HTable table = (HTable) conn.getTable(TableName.valueOf("ns1:t3"));
		// 获取scan对象
		Scan scan = new Scan();
		
		Filter filter = new PrefixFilter(Bytes.toBytes("row1"));
		scan.setFilter(filter);
		
		// 获取扫描器
		ResultScanner scanner = table.getScanner(scan);
		// 通过扫描器获得结果集
		Iterator<Result> it = scanner.iterator();
		while(it.hasNext()){
			Result rs = it.next();
			toStr(rs,"f1","name");
			toStr(rs,"f1","id");
			toStr(rs,"f1","age");
			toStr(rs,"f2","id");
			toStr(rs,"f2","age");
			toStr(rs,"f2","name");
			System.out.println("==============================");
		}
		//System.out.println(System.currentTimeMillis() - start);
	}
	
	/**
	 * 分页过滤器
	 * @throws Exception
	 */
	@Test
	public void pageFilter() throws Exception {
		// 获取hbase配置对象
		Configuration conf = HBaseConfiguration.create();
		// 通过对象创建连接
		Connection conn = ConnectionFactory.createConnection(conf);
		// 得到table对象
		HTable table = (HTable) conn.getTable(TableName.valueOf("ns1:t3"));
		// 获取scan对象
		Scan scan = new Scan();
		
		Filter filter = new PageFilter(15);
		scan.setFilter(filter);
		
		// 获取扫描器
		ResultScanner scanner = table.getScanner(scan);
		// 通过扫描器获得结果集
		Iterator<Result> it = scanner.iterator();
		while(it.hasNext()){
			Result rs = it.next();
			toStr(rs,"f1","name");
			toStr(rs,"f1","id");
			toStr(rs,"f1","age");
			toStr(rs,"f2","id");
			toStr(rs,"f2","age");
			toStr(rs,"f2","name");
			System.out.println("==============================");
		}
		
		//System.out.println(System.currentTimeMillis() - start);
	}
	
	/**
	 * key过滤器
	 * @throws Exception
	 */
	@Test
	public void keyOnlyFilter() throws Exception {
		// 获取hbase配置对象
		Configuration conf = HBaseConfiguration.create();
		// 通过对象创建连接
		Connection conn = ConnectionFactory.createConnection(conf);
		// 得到table对象
		HTable table = (HTable) conn.getTable(TableName.valueOf("ns1:t3"));
		// 获取scan对象
		Scan scan = new Scan();
		
		Filter filter = new KeyOnlyFilter();
		scan.setFilter(filter);
		
		// 获取扫描器
		ResultScanner scanner = table.getScanner(scan);
		// 通过扫描器获得结果集
		Iterator<Result> it = scanner.iterator();
		while(it.hasNext()){
			Result rs = it.next();
			toStr(rs,"f1","name");
			toStr(rs,"f1","id");
			toStr(rs,"f1","age");
			toStr(rs,"f2","id");
			toStr(rs,"f2","age");
			toStr(rs,"f2","name");
			System.out.println("==============================");
		}
		
		//System.out.println(System.currentTimeMillis() - start);
	}
	
	/**
	 * 组合过滤器
	 * @throws Exception
	 */
	@Test
	public void compFilter() throws Exception {
		// 获取hbase配置对象
		Configuration conf = HBaseConfiguration.create();
		// 通过对象创建连接
		Connection conn = ConnectionFactory.createConnection(conf);
		// 得到table对象
		HTable table = (HTable) conn.getTable(TableName.valueOf("ns1:t3"));
		// 获取scan对象
		Scan scan = new Scan();
		
		
		List<Filter> list = new ArrayList<Filter>();
		
		//Filter col = new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("name")));
		//Filter val = new ValueFilter(CompareOp.EQUAL, new RegexStringComparator("tom[0123456789]"));
		Filter single = new SingleColumnValueFilter(Bytes.toBytes("f1"), Bytes.toBytes("name"), CompareOp.EQUAL, new RegexStringComparator("tom[0123456789]"));
		Filter cf = new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("f1")));
		
		list.add(single);
		list.add(cf);
		
		FilterList fl = new FilterList(list);
		
		scan.setFilter(fl);
		
		// 获取扫描器
		ResultScanner scanner = table.getScanner(scan);
		// 通过扫描器获得结果集
		Iterator<Result> it = scanner.iterator();
		while(it.hasNext()){
			Result rs = it.next();
			toStr(rs,"f1","name");
			toStr(rs,"f1","id");
			toStr(rs,"f1","age");
			toStr(rs,"f2","id");
			toStr(rs,"f2","age");
			toStr(rs,"f2","name");
			System.out.println("==============================");
		}
		
		//System.out.println(System.currentTimeMillis() - start);
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	private void toStr(Result rs, String cf, String col) {
		
		List<Cell> cells = rs.getColumnCells(Bytes.toBytes(cf), Bytes.toBytes(col));
		for(Cell cell : cells){
			String row = new String(CellUtil.cloneRow(cell));
			String v = new String(CellUtil.cloneValue(cell));
			
			System.out.println( row+"/"+cf+"/"+col+":"+ v );	
		}

	}

}
