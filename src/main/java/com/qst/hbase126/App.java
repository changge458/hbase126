package com.qst.hbase126;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

/**
 * Hello world!
 *
 */
public class App {
	/**
	 * 创建名字空间
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// 获取hbase配置对象
		Configuration conf = HBaseConfiguration.create();
		// 通过对象创建连接
		Connection conn = ConnectionFactory.createConnection(conf);
		// 得到hbase管理员
		Admin admin = conn.getAdmin();
		// 构建一个名字空间描述符
		NamespaceDescriptor nsd = NamespaceDescriptor.create("ns3").build();
		// 通过管理员创建名字空间
		admin.createNamespace(nsd);
		conn.close();
		System.out.println("ok");

	}

	/**
	 * 创建表
	 * 
	 * @throws Exception
	 */
	@Test
	public void createTable() throws Exception {
		// 获取hbase配置对象
		Configuration conf = HBaseConfiguration.create();
		// 通过对象创建连接
		Connection conn = ConnectionFactory.createConnection(conf);
		// 得到hbase管理员
		Admin admin = conn.getAdmin();
		// 通过valueof获取到tableName对象
		TableName name = TableName.valueOf("ns1:t4");
		// 得到table的对象
		HTableDescriptor table = new HTableDescriptor(name);
		// 在table中添加列族
		table.addFamily(new HColumnDescriptor("f1"));
		table.addFamily(new HColumnDescriptor("f2"));
		// 通过admin创建table
		admin.createTable(table);
		conn.close();
		System.out.println("ok");

	}

	/**
	 * 添加字段
	 * 
	 * @throws Exception
	 */
	@Test
	public void put() throws Exception {
		// 获取hbase配置对象
		Configuration conf = HBaseConfiguration.create();
		// 通过对象创建连接
		Connection conn = ConnectionFactory.createConnection(conf);
		// 通过连接，得到table对象
		Table table = conn.getTable(TableName.valueOf("ns1:t1"));
		// 创建put动作
		Put put = new Put(Bytes.toBytes("row1"));
		long start = System.currentTimeMillis();
		for( int i = 1 ; i <= 5 ; i++){
			
			put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("name"), Bytes.toBytes("tom"+i));
			// 添加数据
			table.put(put);
		}
		System.out.println(System.currentTimeMillis() - start);

	}

	/**
	 * hbase数据删除
	 */
	@Test
	public void delete() throws Exception {
		// 获取hbase配置对象
		Configuration conf = HBaseConfiguration.create();
		// 通过对象创建连接
		Connection conn = ConnectionFactory.createConnection(conf);
		// 通过连接，得到table对象
		Table table = conn.getTable(TableName.valueOf("ns3:t1"));
		// 创建delete动作
		Delete delete = new Delete(Bytes.toBytes("row1"));
		delete.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("name"));
		delete.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("age"));
		delete.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("id"));

		table.delete(delete);
		System.out.println("ok");
	}

	/**
	 * hbase表删除
	 * 
	 * @throws Exception
	 */
	@Test
	public void deleteTable() throws Exception {
		// 获取hbase配置对象
		Configuration conf = HBaseConfiguration.create();
		// 通过对象创建连接
		Connection conn = ConnectionFactory.createConnection(conf);

		// 得到hbase管理员
		Admin admin = conn.getAdmin();
		admin.disableTable(TableName.valueOf("ns3:t1"));
		admin.deleteTable(TableName.valueOf("ns3:t1"));
		System.out.println("ok");
	}

	/**
	 * get操作
	 * 
	 * @throws Exception
	 */
	@Test
	public void get() throws Exception {
		// 获取hbase配置对象
		Configuration conf = HBaseConfiguration.create();
		// 通过对象创建连接
		Connection conn = ConnectionFactory.createConnection(conf);

		Table table = conn.getTable(TableName.valueOf("ns1:t1"));
		Get get = new Get(Bytes.toBytes("row1"));
		get.setMaxVersions(5);
		get.setTimeRange(1510556806745L, 1510556896648L);
		Result rs = table.get(get);
		List<Cell> cells = rs.getColumnCells(Bytes.toBytes("user"), Bytes.toBytes("age"));
		for (Cell cell : cells) {
			System.out.println(new String(CellUtil.cloneValue(cell)));
		}

	}

	/**
	 * hbase名字空间
	 * 
	 * @throws Exception
	 */

	@Test
	public void deleteNS() throws Exception {
		// 获取hbase配置对象
		Configuration conf = HBaseConfiguration.create();
		// 通过对象创建连接
		Connection conn = ConnectionFactory.createConnection(conf);

		// 得到hbase管理员
		Admin admin = conn.getAdmin();

		admin.deleteNamespace("ns3");
		System.out.println("ok");
	}

	/**
	 * 扫描hbase数据
	 * 
	 * @throws Exception
	 */
	@Test
	public void scan() throws Exception {
		// 获取hbase配置对象
		Configuration conf = HBaseConfiguration.create();
		// 通过对象创建连接
		Connection conn = ConnectionFactory.createConnection(conf);
		// 得到table对象
		Table table = conn.getTable(TableName.valueOf("ns1:t1"));
		// 得到扫描器
		ResultScanner scanner = table.getScanner(Bytes.toBytes("user"));
		// 通过扫描器获得结果集
		Iterator<Result> rs = scanner.iterator();
		// 迭代结果集，返回可导航的map
		while (rs.hasNext()) {
			NavigableMap<byte[], byte[]> map = rs.next().getFamilyMap(Bytes.toBytes("user"));
			// 可导航的map封装了sortedMap，后者可以直接获取k，v
			for (Entry<byte[], byte[]> entry : map.entrySet()) {
				System.out.println(new String(entry.getKey()) + ":" + new String(entry.getValue()));
			}
		}

	}

	/**
	 * 扫描hbase数据,原生扫描raw
	 * 
	 * @throws Exception
	 */
	@Test
	public void rawScan() throws Exception {
		// 获取hbase配置对象
		Configuration conf = HBaseConfiguration.create();
		// 通过对象创建连接
		Connection conn = ConnectionFactory.createConnection(conf);
		// 得到table对象
		HTable table = (HTable) conn.getTable(TableName.valueOf("ns1:t1"));
		// 获取scan对象
		Scan scan = new Scan();
		scan.setCaching(50000);
		scan.addFamily(Bytes.toBytes("user"));
		scan.setRaw(true);
		scan.setMaxVersions(10);
		// 获取扫描器
		ResultScanner scanner = table.getScanner(scan);
		// 通过扫描器获得结果集
		Iterator<Result> rs = scanner.iterator();
		// 迭代结果集，返回可导航的map
		while (rs.hasNext()) {
			List<Cell> cells = rs.next().getColumnCells(Bytes.toBytes("user"), Bytes.toBytes("age"));
			for(Cell cell : cells){
				new String(CellUtil.cloneValue(cell));
				CellUtil.getCellKeyAsString(cell);
			}
			
		}

	}
	
	
	
	
	

}
