package com.qst.hbase126;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class TestBatch {

	public static void main(String[] args) {
		try {
			putBatch("name", 1);
			putBatch("pass", 2);
			putBatch("email", 3);
			putBatch("nickname", 4);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 批量put
	 * 
	 * @throws Exception
	 */
	public static void putBatch(String k, int v) throws Exception {
		// 获取hbase配置对象
		Configuration conf = HBaseConfiguration.create();
		// 通过对象创建连接
		Connection conn = ConnectionFactory.createConnection(conf);
		// 通过连接，得到table对象
		HTable table = (HTable) conn.getTable(TableName.valueOf("duowan"));
		// 创建put动作
		List<Put> puts = new ArrayList<Put>();
		long start = System.currentTimeMillis();
		BufferedReader br = new BufferedReader(new FileReader("D:/wc/duowan_user2.txt"));
		String line = null;
		int count = 0;
		while ((line = br.readLine()) != null) {
			System.out.println(count++);
			line = br.readLine();
			if (line != null && line.length() > 0) {

				String[] arr = line.split("\t");
				if (arr.length >= 3) {

					Put put = new Put(Bytes.toBytes(arr[0].length() != 0 ? arr[0] : "null"));

					try {
						byte[] cf = Bytes.toBytes("f1");
						byte[] col = Bytes.toBytes(k);
						byte[] val = Bytes.toBytes(arr[v].length() != 0 ? arr[v] : "null");
						put.addColumn(cf, col, val);

					} catch (Exception e) {
						e.printStackTrace();
						continue;
					}
					puts.add(put);
				}
			}
		}
		table.put(puts);
		System.out.println(System.currentTimeMillis() - start);
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

		long start = System.currentTimeMillis();
		for (int i = 1; i <= 1000; i++) {
			Put put = new Put(Bytes.toBytes("row" + i));
			put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("name"), Bytes.toBytes("tom" + i));
			// 添加数据
			table.put(put);
			System.out.println(i);
		}
		System.out.println(System.currentTimeMillis() - start);

	}

	/**
	 * 扫描hbase数据,设置扫描缓存
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
		HTable table = (HTable) conn.getTable(TableName.valueOf("ns1:t3"));

		// 获取scan对象
		Scan scan = new Scan();

		scan.setBatch(5);
		scan.setCaching(2);
		// 获取扫描器
		ResultScanner scanner = table.getScanner(scan);
		// 通过扫描器获得结果集
		Iterator<Result> it = scanner.iterator();
		// 迭代结果集，返回可导航的map
		// long start = System.currentTimeMillis();
		while (it.hasNext()) {
			Result rs = it.next();
			toStr(rs, "f1", "name");
			toStr(rs, "f1", "id");
			toStr(rs, "f1", "age");
			toStr(rs, "f2", "id");
			toStr(rs, "f2", "age");
			toStr(rs, "f2", "name");
			System.out.println("==============================");
		}
		// System.out.println(System.currentTimeMillis() - start);

	}

	private void toStr(Result rs, String cf, String col) {

		List<Cell> cells = rs.getColumnCells(Bytes.toBytes(cf), Bytes.toBytes(col));
		for (Cell cell : cells) {
			String row = new String(CellUtil.cloneRow(cell));
			String v = new String(CellUtil.cloneValue(cell));

			System.out.println(row + "/" + cf + "/" + col + ":" + v);
		}
	}

}
