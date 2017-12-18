package com.qst.phoenix413;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Test;

public class TestPhoenix {

	public static void main(String[] args) throws Exception {

		String driver = "org.apache.phoenix.jdbc.PhoenixDriver";
		String url = "jdbc:phoenix:s202,s203,s204";

		Class.forName(driver);

		Connection conn = DriverManager.getConnection(url);

		Statement st = conn.createStatement();

		st.execute("create table duowan(id integer primary key, name varchar,pass varchar,email varchar,nickname varchar)");

		st.close();
		conn.close();
		System.out.println("ok");

	}

	@Test
	public void insert() throws Exception {

		String driver = "org.apache.phoenix.jdbc.PhoenixDriver";
		String url = "jdbc:phoenix:s202,s203,s204";

		Class.forName(driver);

		Connection conn = DriverManager.getConnection(url);

		conn.setAutoCommit(true);
		
		Statement st = conn.createStatement();

		st.executeUpdate("upsert into tt values(1,'tomasLee',30)");

		st.close();
		conn.close();
		System.out.println("ok");

	}
	
	@Test
	public void select() throws Exception {

		String driver = "org.apache.phoenix.jdbc.PhoenixDriver";
		String url = "jdbc:phoenix:s202,s203,s204";

		Class.forName(driver);

		Connection conn = DriverManager.getConnection(url);

		conn.setAutoCommit(true);
		
		Statement st = conn.createStatement();

		ResultSet rs = st.executeQuery("select * from t1");

		while(rs.next()){
			int id = rs.getInt(1);
			String name = rs.getString(2);
			int age = rs.getInt(3);
			System.out.println(id+"/"+name+"/"+age);
		}
		
		st.close();
		conn.close();
		System.out.println("ok");

	}

}
