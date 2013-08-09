package com.gw.hiveclient;

import java.sql.*;

/**
 * HiveCommon is a common class about hive operator, function: <li>you can use
 * this to connect or close hive server <li>exec hsql and return data from hive
 * <li>create table or extern table <li>load data to hive table from local or
 * hdfs
 * 
 * @author huangfengxiao 2013.08.01
 */
class HiveCommon {
	private String m_url;
	private String m_user;
	private String m_pwd;

	private final String DriverName = "org.apache.hadoop.hive.jdbc.HiveDriver";

	private Connection m_conn = null;
	private Statement m_stmt = null;

	/**
	 * The constructor of HiveCommon
	 * 
	 * @param url
	 *            the hive server address
	 * @param user
	 *            user
	 * @param pwd
	 *            password
	 */
	HiveCommon(String url, String user, String pwd) {
		this.m_url = url;
		this.m_user = user;
		this.m_pwd = pwd;
	}

	/**
	 * return the connect info about url, user and pwd
	 */
	public String toString() {
		return "ip=" + m_url + " user=" + m_user + " pwd=" + m_pwd + "\n";
	}

	/**
	 * connect to hive server, you must open the hive server at first.
	 * 
	 * @return if true is sucesses or false faild
	 */
	public boolean ConnHive() {
		boolean bRet = false;
		try {
			Class.forName(DriverName);
			if (m_conn == null) {
				m_conn = DriverManager.getConnection(m_url, m_user, m_pwd);
			}
			if (m_stmt == null) {
				m_stmt = m_conn.createStatement();
			}
			bRet = true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return bRet;
	}

	/**
	 * exec query hql and return data package ResultSet
	 * 
	 * @param hql
	 * @return if null is faild or sucesses
	 */
	@SuppressWarnings("finally")
	public ResultSet ExecQuery(String hql) {
		ResultSet res = null;
		try {
			res = m_stmt.executeQuery(hql);
		} catch (Exception e) {
			System.out.println("Connection faild!e=" + e.getMessage());
			e.printStackTrace();
		} finally {
			return res;
		}
	}

	/**
	 * exec not query hql (ddl)
	 * 
	 * @param hql
	 * @return if false is faild or sucesses
	 */
	@Deprecated
	@SuppressWarnings("finally")
	public boolean ExecNotQuery(String hql) {
		boolean ret = false;
		try {
			m_stmt.executeQuery(hql);
			ret = true;
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			return ret;
		}
	}

	/**
	 * 
	 * @return if true is sucesses or false faild
	 */
	@SuppressWarnings("finally")
	public boolean Close() {
		boolean ret = true;
		try {
			if (null != m_stmt) {
				m_stmt.close();
			}
			if (null != m_conn) {
				m_conn.close();
			}
		} catch (Exception e) {
			ret = false;
			e.printStackTrace();
		} finally {
			return ret;
		}
	}
}
