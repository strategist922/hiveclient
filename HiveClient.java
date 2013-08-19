package com.gw.hiveclient;

import java.awt.Button;
import java.awt.Color;
import java.awt.FlowLayout;
import java.awt.Frame;
import java.awt.GridLayout;
import java.awt.Label;
import java.awt.Panel;
import java.awt.TextArea;
import java.awt.TextField;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;

/**
 * the query thread for noblock query in windows
 * 
 * @author huangfengxiao
 * @email huangfengxiao@gw.com.cn
 * @version date:2013年8月15日 下午4:10:03
 */
class myquery implements Runnable {
	private HiveClient rn;

	public myquery(HiveClient rn) {
		this.rn = rn;
	}

	@Override
	public void run() {
		rn.b_query.setLabel("waitting");
		rn.b_query.setEnabled(false);
		rn.t_mesg.setText("hql execing in hadoop, please waiting...");
		ResultSet res = null;
		try {
			res = rn.jdbc.ExecQuery(rn.t_hql.getText());
		} catch (SQLException e) {
			rn.t_mesg.setText(e.getMessage());
			rn.b_query.setEnabled(true);
			rn.b_query.setLabel("exec hql");
			return;
		}

		rn.t_mesg.setText("read exec result, please waiting...");
		rn.b_stop.setEnabled(true);
		rn.ta_res.setText("");

		try {
			ResultSetMetaData rsmd = res.getMetaData();
			int columnCount = rsmd.getColumnCount() + 1;
			int row = 1;

			for (int i = 1; i <= columnCount - 1; i++) {
				rn.ta_res.append("\t" + rsmd.getColumnName(i) + "\t");
			}
			rn.ta_res.append("\n");

			String temp = "";
			while (res.next() && !rn.isstop) {
				temp += String.valueOf(row++);
				for (int i = 1; i < columnCount; i++) {
					temp += "\t" + res.getString(i) + "\t";
				}
				temp += "\n";
				if (row % 1000 == 0) {
					rn.ta_res.append(temp);
					temp = "";
				}
			}
			rn.ta_res.append(temp);
			rn.t_mesg.setText("hql exec sucess!");
		} catch (Exception ex) {
			rn.t_mesg.setText("read res faild!" + ex.getMessage());
			rn.b_query.setEnabled(true);
		}

		if (rn.isstop) {
			rn.t_mesg.setText("hql exec sucess, but be stopped!");
			rn.isstop = false;
		}
		rn.b_stop.setEnabled(false);
		rn.b_query.setEnabled(true);
		rn.b_query.setLabel("exec hql");
	}

}

/**
 * 
 * hiveclient 0.1.0 <li>exec hql <li>create table or external table in hive <li>backup hive data to hdfs or local <li>import hive data to mysql or
 * imort mysql data to hive
 * 
 * @author huangfengxiao
 * @email huangfengxiao@gw.com.cn
 * @version date:2013年8月15日 下午4:07:05
 */
public class HiveClient extends WindowAdapter implements ActionListener {

	public HiveCommon jdbc;
	public boolean isstop = false;
	private String defaulturl = "";
	private String defaultusr = "";
	private String defaultpwd = "";
	private int right = 0;

	public Frame f = new Frame();

	// head panel
	private Panel p_head = new Panel();
	public Label l_title = new Label("DZH Hive Client 1.0", Label.CENTER);
	private Label l_bank = new Label();
	private Panel p_guest = new Panel();
	private TextField t_guestname = new TextField(10);
	private TextField t_guestpwd = new TextField(10);
	public Button b_login = new Button("Login");
	public TextField t_mesg = new TextField();

	// connect panel
	private Panel p_conn = new Panel();
	private Label l_conn = new Label("please write connect hive info: (hive url port, mysql root, mysql pwd)", Label.LEFT);
	private Panel p_conncur = new Panel();
	private TextField t_url = new TextField(39);
	private TextField t_user = new TextField(39);
	private TextField t_pwd = new TextField(39);
	private Button b_conn = new Button();
	private Label l_bank2 = new Label("<1>Hive query language <2>append hive table", Label.LEFT);

	// query panel
	private Panel p_query = new Panel();
	private Panel p_query1 = new Panel();
	public TextField t_hql = new TextField("show tables");
	private Panel p_querycur = new Panel();
	public Button b_query = new Button("exec hql");
	public Button b_stop = new Button("stop");
	private Panel p_query2 = new Panel();
	private Panel p_query3 = new Panel();
	public TextField t_hdfsurl = new TextField("hdfs://hfxcentos:9571", 39);
	public TextField t_mrurl = new TextField("hfxcentos:9572", 39);
	public TextField t_appendpath = new TextField("/user/hive/warehouse/login/1.txt", 39);
	public TextArea t_appenddata = new TextArea("yourdata");
	public Button b_append = new Button("append");

	// textarea
	public TextArea ta_res = new TextArea();

	public HiveClient(String defaulturl, String defaultusr, String defaultpwd) {
		this.defaulturl = defaulturl;
		this.defaultusr = defaultusr;
		this.defaultpwd = defaultpwd;
	}

	public void init() throws SQLException {
		f = new Frame();
		f.setTitle("hiveclient 0.1.0");
		f.setSize(1000, 700);
		f.setVisible(true);
		f.setLocationRelativeTo(null);
		f.addWindowListener(this);
		f.setVisible(true);
		f.setLayout(new GridLayout(4, 2));
		f.setResizable(false);
		l_title.setBackground(Color.gray);
		l_bank.setBackground(Color.gray);
		t_mesg.setText("[logs:]");
		t_mesg.setEditable(false);

		p_head.add(l_title);
		p_head.add(l_bank);
		p_head.add(p_guest);
		p_guest.add(new Label("loginname: "));
		p_guest.add(t_guestname);
		p_guest.add(new Label("password: "));
		p_guest.add(t_guestpwd);
		p_guest.add(b_login);
		p_guest.add(new Label("[Note] if you hive no logininfo, please call 10086."));
		p_head.add(t_mesg);
		b_login.addActionListener(this);
		p_head.setLayout(new GridLayout(4, 1));

		f.add(p_head);
		f.add(p_conn);
		f.add(p_query);
		f.add(ta_res);

		p_conn.setEnabled(false);
		p_query.setEnabled(false);
		ta_res.setEnabled(false);
		t_mesg.setText("Please input your login name and password!");

	}

	public void conn() {
		t_url.setText("jdbc:hive://hfxcentos:8899/default");
		t_user.setText("root");
		t_pwd.setText("111111");
		b_conn.setLabel("connect");
		b_conn.addActionListener(this);

		p_conn.add(l_conn);
		p_conncur.add(t_url);
		p_conncur.add(t_user);
		p_conncur.add(t_pwd);
		p_conncur.add(b_conn);
		p_conn.add(p_conncur);
		p_conn.add(l_bank2);

		p_conncur.setLayout(new FlowLayout(FlowLayout.LEFT));
		p_conn.setLayout(new GridLayout(3, 1));
	}

	public void hquery() {

		b_query.addActionListener(this);
		b_stop.addActionListener(this);
		b_append.addActionListener(this);
		b_stop.setEnabled(false);

		p_query.add(p_query2);
		p_query.add(p_query1);

		Panel p_appendcur1 = new Panel();
		Panel p_appendcur2 = new Panel();
		p_query2.add(p_appendcur1);
		p_query2.add(p_appendcur2);
		p_query2.setLayout(new FlowLayout(FlowLayout.LEFT));
		p_query2.setLayout(new GridLayout(1, 2));
		p_appendcur1.add(p_query3);
		p_query3.add(t_hdfsurl);
		p_query3.add(t_mrurl);
		p_query3.setLayout(new GridLayout(2, 1));
		p_appendcur1.add(t_appendpath);
		p_appendcur2.add(t_appenddata);
		p_appendcur2.add(b_append);
		p_appendcur1.setLayout(new GridLayout(1, 2));
		p_appendcur2.setLayout(new GridLayout(1, 2));

		p_query1.add(t_hql);
		p_query1.add(p_querycur);
		p_query1.setLayout(new FlowLayout(FlowLayout.LEFT));
		p_query1.setLayout(new GridLayout(1, 2));
		p_querycur.add(b_query);
		p_querycur.add(b_stop);
		p_querycur.setLayout(new GridLayout(1, 2));

		p_query.setLayout(new GridLayout(2, 3));

		ta_res.setSize(80, 80);
		ta_res.setText("exec here!");
		ta_res.setEditable(false);
	}

	public void windowClosing(WindowEvent e) {
		f.dispose();
	}

	public void actionPerformed(ActionEvent e) {
		// login event
		if (e.getSource().equals(b_login) == true) {
			try {
				right = checklogin(t_guestname.getText(), t_guestpwd.getText());
				if (right == 1 || right == 2) {
					p_conn.setEnabled(true);
					p_query.setEnabled(true);
					ta_res.setEnabled(true);
					String sright = (right == 1) ? "r" : "r/w";
					t_mesg.setText("your right is " + sright);
				} else if (right == 0) {
					t_mesg.setText("login info error!");
				}
			} catch (SQLException e1) {
				t_mesg.setText(e1.getMessage());
				e1.printStackTrace();
			}
		}

		// conn and query
		if (e.getSource().equals(b_conn) == true) {
			if (b_conn.getLabel().equals("connect") == true) {

				jdbc = new HiveCommon(t_url.getText(), t_user.getText(), t_pwd.getText());

				if (!jdbc.ConnHive()) {
					t_mesg.setText("hive connect faild:=" + jdbc.toString());
					return;
				}
				t_mesg.setText("hive connect sucess:=" + jdbc.toString());
				b_conn.setLabel("close");
			} else {
				jdbc.Close();
				t_mesg.setText("hive close sucess:=" + jdbc.toString());
				b_conn.setLabel("connect");
			}
		} else if (e.getSource().equals(b_query) == true) {
			if (t_hql.getText().equals("")) {
				t_mesg.setText("please input you hql!");
				return;
			}
			if (right == 1) {
				String xiaoxie = t_hql.getText().toLowerCase();
				if (xiaoxie.contentEquals("load") || xiaoxie.contentEquals("create")) {
					t_mesg.setText("you have no right to w!");
					return;
				}
			}
			if (jdbc == null) {
				t_mesg.setText("you must connect hive at first!");
				return;
			}
			myquery runable = new myquery(this);
			Thread t = new Thread(runable);
			t.start();
		} else if (e.getSource().equals(b_stop) == true) {
			isstop = true;
		}

		// append
		if (e.getSource().equals(b_append) == true) {
			if (right == 1) {
				t_mesg.setText("you can not append, because your right is 1!");
				return;
			}
			Configuration conf = new Configuration();
			conf.set("fs.default.name", t_hdfsurl.getText());
			conf.set("mapred.job.tracker", t_mrurl.getText());
			conf.addResource(t_hdfsurl.getText());
			HdfsCommon hdfs = HdfsCommon.getInstance(conf);

			try {
				hdfs.appendFile(t_appendpath.getText(), t_appenddata.getText() + "\n", "utf-8");
				t_mesg.setText("append sucesses!");
			} catch (IOException e1) {
				t_mesg.setText(e1.getMessage());
				e1.printStackTrace();
			}
		}
	}

	/**
	 * check login name
	 * 
	 * @param name
	 * @param pwd
	 * @return 1 read, 2 read and write, 0 no right to do anything, -1 error
	 * @throws SQLException
	 */
	private int checklogin(String name, String pwd) throws SQLException {
		HiveCommon h = new HiveCommon(defaulturl, defaultusr, defaultpwd);
		int ret = 0;
		if (!h.ConnHive()) {
			ret = -1; // conn error
		}

		ResultSet res = h.ExecQuery("select * from login");
		if (res == null) {
			ret = -1; // exec error
		} else {
			while (res.next()) {
				if (name.equals(res.getString(1))) {
					if (pwd.equals(res.getString(2))) {
						ret = Integer.parseInt(res.getString(3));
						break;
					}
				}
			}
		}
		h.Close();
		return ret;
	}

	public static void main(String args[]) throws SQLException {
		HiveClient h = new HiveClient("jdbc:hive://hfxcentos:8899/default", "root", "111111");
		h.init();
		h.conn();
		h.hquery();
	}
}
