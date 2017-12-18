package com.qst.hbase126;

import java.io.IOException;
import java.util.List;
import java.util.NavigableMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

public class MyRegionObserver extends BaseRegionObserver {

	private static final String TABLE_NAME = "ns1:t4";
	private static final String CF1 = "f1";
	private static final String CF2 = "f2";

	/**
	 * 写前检查 1、检查表是否匹配 2、设置只读
	 * 
	 * put 'ns1:t4','tom-tomas','f1:name','tom,tomas'
	 */
	@Override
	public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability)
			throws IOException {

		// 表不匹配则报异常
		//ns1:t1
		String table = e.getEnvironment().getRegionInfo().getTable().getNameAsString();
		if (!table.equals(TABLE_NAME)) {
			try {
				throw new Exception("Table Name Miss Match");
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			return;
		}

		NavigableMap<byte[], List<Cell>> map = put.getFamilyCellMap();

		/**
		 * 收到来自f1的消息添加，f2会自动添加 0、cf;col;val 1、table 2、new Put 3、put.addColumn()
		 * 4、table.put(put)
		 * 
		 */
		byte[] row;
		byte[] cf;
		byte[] col;
		byte[] val;
		Table t = e.getEnvironment().getTable(TableName.valueOf(TABLE_NAME));
		for (List<Cell> cells : map.values()) {
			for (Cell cell : cells) {
				//byte[]: tom-tomas
				row = CellUtil.cloneRow(cell);
				//byte[]: f1
				cf = CellUtil.cloneFamily(cell);
				//byte[]: name
				col = CellUtil.cloneQualifier(cell);
				//byte[]: tom,tomas
				val = CellUtil.cloneValue(cell);
				// tom-tomas
				String r = new String(row);
				// f1
				String family = new String(cf);
				// tom,tomas
				String value = new String(val);
				if (family.equals(CF1) && r.matches(".+\\-+.+")) {
					// 变换rowkey
					String[] arr = r.split("\\-");
					//tom
					String me = arr[0];
					//tomas
					String it = arr[1];
					// 修改value
					String[] arr2 = value.split(",");
					// tomas
					String it2 = arr2[0];
					String me2 = arr2[1];
					// new put(tomas-tom)
					Put putFans = new Put(Bytes.toBytes(it + "-" + me));
					Put putInterest = new Put(row);
					//
					putInterest.addColumn(cf, col, Bytes.toBytes(it2));
					putFans.addColumn(Bytes.toBytes(CF2), col, Bytes.toBytes(me2));
					t.put(putFans);
					t.put(putInterest);
				}
			}
		}

	}

//	@Override
//	public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability)
//			throws IOException {
//
//		NavigableMap<byte[], List<Cell>> map = put.getFamilyCellMap();
//		byte[] row;
//		byte[] cf;
//		byte[] col;
//		byte[] val;
//		Table t = e.getEnvironment().getTable(TableName.valueOf(TABLE_NAME));
//		for (List<Cell> cells : map.values()) {
//			for (Cell cell : cells) {
//				row = CellUtil.cloneRow(cell);
//				cf = CellUtil.cloneFamily(cell);
//				col = CellUtil.cloneQualifier(cell);
//				val = CellUtil.cloneValue(cell);
//				String r = new String(row);
//				String family = new String(cf);
//				String value = new String(val);
//				if (family.equals(CF1) && r.matches(".+\\-+.+")) {
//
//					// 修改value
//					String[] arr = value.split(",");
//					String me = arr[0];
//
//					Put putInterest = new Put(row);
//					putInterest.addColumn(cf, col, Bytes.toBytes(me));
//					t.put(putInterest);
//				}
//			}
//		}
//
//	}

	/**
	 * 删除f1，f2随之删除
	 */
	@Override
	public void preDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit,
			Durability durability) throws IOException {

		NavigableMap<byte[], List<Cell>> map = delete.getFamilyCellMap();

		byte[] row;
		byte[] cf;
		byte[] col;
		byte[] val;
		Table t = e.getEnvironment().getTable(TableName.valueOf(TABLE_NAME));
		for (List<Cell> cells : map.values()) {
			for (Cell cell : cells) {
				row = CellUtil.cloneRow(cell);
				cf = CellUtil.cloneFamily(cell);
				col = CellUtil.cloneQualifier(cell);
				val = CellUtil.cloneValue(cell);
				String family = new String(cf);
				if (family.equals(CF1)) {
					Delete del = new Delete(row);
					del.addColumn(Bytes.toBytes(CF2), col);
					t.delete(del);
				}
			}
		}
	}

}
