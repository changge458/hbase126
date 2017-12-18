package com.qst.phoenix.udf;

import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.function.ScalarFunction;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;

public class SplitUDF extends ScalarFunction {
	public static final String FUNC_NAME = "split";

	public SplitUDF() {
	}

	public SplitUDF(List<Expression> children) throws SQLException {
		super(children);
	}

	public String getName() {
		return FUNC_NAME;
	}

	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
		
		String key = tuple.toString();
		
		String t = key.substring(0, 1);
		//写入
		ptr.set(PVarchar.INSTANCE.toBytes(t));
		return true;

	}

	public PDataType getDataType() {
		// TODO Auto-generated method stub
		return PVarchar.INSTANCE;
	}

	

}
