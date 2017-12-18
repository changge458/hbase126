package com.qst.hbase126;

import java.io.IOException;
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
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class TestFilter2 {

	public static void main(String[] args) throws Exception {

		Configuration conf = HBaseConfiguration.create();
		Connection conn = ConnectionFactory.createConnection(conf);
		HTable table = (HTable) conn.getTable(TableName.valueOf("ns1:t3"));
		Scan scan = new Scan();
		

		/**
		 * 1、单列值tom[0123456789] + family(f1) 2、单列值age < 20 + family(f1)
		 * 
		 */
		Filter f1 = new SingleColumnValueFilter(bytes("f1"), bytes("name"), CompareOp.EQUAL,
				new RegexStringComparator("tom[0123456789]"));

		Filter f2 = new SingleColumnValueFilter(bytes("f1"), bytes("age"), CompareOp.GREATER,
				new BinaryComparator(bytes("20")));

		Filter f3 = new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(bytes("f1")));

		List<Filter> list = new ArrayList<Filter>();
		list.add(f1);
		list.add(f2);
		list.add(f3);

		FilterList fl = new FilterList(list);

		scan.setFilter(fl);

		ResultScanner scanner = table.getScanner(scan);
		Iterator<Result> it = scanner.iterator();
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

	}

	public static byte[] bytes(String str) {
		return Bytes.toBytes(str);
	}

	public static byte[] num(String str) {
		Integer i = Integer.parseInt(str);
		Integer j = Integer.MAX_VALUE - i;
		return Bytes.toBytes(j);
	}

	private static void toStr(Result rs, String cf, String col) {

		List<Cell> cells = rs.getColumnCells(Bytes.toBytes(cf), Bytes.toBytes(col));
		for (Cell cell : cells) {
			String row = new String(CellUtil.cloneRow(cell));
			String v = new String(CellUtil.cloneValue(cell));

			System.out.println(row + "/" + cf + "/" + col + ":" + v);
		}

	}

}
