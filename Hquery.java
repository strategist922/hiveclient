package com.gw.hiveclient;

/**
 * hive query client 1.0 by HuangFX
 * test 
 */
import java.awt.*;
import java.awt.event.*;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

class myquery implements Runnable {
	private Hquery rn;

	public myquery(Hquery rn) {
		this.rn = rn;
	}

	@Override
	public void run() {
		rn.b_query.setLabel("waitting");
		rn.b_query.setEnabled(false);
		rn.t_mesg.setText("hql execing in hadoop, please waiting!");
		ResultSet res = rn.jdbc.ExecQuery(rn.ta_hql.getText());
		if (res != null) {
			rn.t_mesg.setText("read exec result, please waiting!");
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

				// while (res.next() && !rn.isstop) {
				// rn.ta_res.append(String.valueOf(row++));
				// for (int i = 1; i < columnCount; i++) {
				// rn.ta_res.append("\t" + res.getString(i) + "\t");
				// }
				// rn.ta_res.append("\n");
				// }
				rn.t_mesg.setText("hql exec sucess!");
			} catch (Exception ex) {
				rn.t_mesg.setText("read res faild!" + ex.getMessage());
				rn.b_query.setEnabled(true);
			}
		} else {
			rn.t_mesg.setText("hql exec faild!");
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
 * Hquery : hive client 1.0 main
 * @author huangfengxiao
 *
 */
public class Hquery extends WindowAdapter implements ActionListener {

	public HiveCommon jdbc;
	public boolean isstop = false;

	public Frame f = new Frame();
	
	private Panel p_head = new Panel();
	public Label l_title = new Label("Hive Client 1.0", Label.CENTER);
	private Label l_bank = new Label();
	public TextField t_mesg = new TextField();

	private Panel p_conn = new Panel();
	private Label l_conn = new Label(
			"please write connect hive info: (hive url port, mysql root, mysql pwd)",
			Label.LEFT);
	private Panel p_conncur = new Panel();
	private TextField t_url = new TextField(39);
	private TextField t_user = new TextField(39);
	private TextField t_pwd = new TextField(39);
	private Button b_conn = new Button();
	private Label l_bank2 = new Label(
			"please write hive query language : do not use \";\" ", Label.LEFT);

	private Panel p_query = new Panel();
	public TextArea ta_hql = new TextArea("show tables");
	private Panel p_querycur = new Panel();
	public Button b_query = new Button("exec hql");
	public Button b_stop = new Button("stop");

	public TextArea ta_res = new TextArea();

	public void init() {
		f = new Frame();
		f.setTitle("hive client 1.0");
		f.setSize(1000, 700);
		f.setVisible(true);
		f.setLocationRelativeTo(null);
		f.addWindowListener(this);
		f.setVisible(true);
		f.setLayout(new GridLayout(4, 2));
		f.setResizable(false);
		l_title.setBackground(Color.gray);
		l_bank.setBackground(Color.gray);
		t_mesg.setText("log info...");
		t_mesg.setEditable(false);

		p_head.add(l_title);
		p_head.add(l_bank);
		p_head.add(t_mesg);

		p_head.setLayout(new GridLayout(3, 1));

		f.add(p_head);
		f.add(p_conn);
		f.add(p_query);
		f.add(ta_res);
	}

	public void conn() {
		t_url.setText("jdbc:hive://hadoop.cluster.master:8899/default");
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
		// ta_hql.setText("show tables");

		b_query.addActionListener(this);
		b_stop.addActionListener(this);
		b_stop.setEnabled(false);

		p_query.add(ta_hql);
		p_query.add(p_querycur);
		p_querycur.add(b_query);
		p_querycur.add(b_stop);
		p_query.setLayout(new FlowLayout(FlowLayout.LEFT));
		p_query.setLayout(new GridLayout(1, 2));
		p_querycur.setLayout(new GridLayout(1, 2));

		ta_res.setSize(80, 80);
		ta_res.setText("exec here!");
		ta_res.setEditable(false);

	}

	public void windowClosing(WindowEvent e) {
		// System.exit(0);
		f.dispose();
	}

	public static void main(String args[]) {
		Hquery h = new Hquery();
		h.init();
		h.conn();
		h.hquery();
	}

	public void actionPerformed(ActionEvent e) {
		if (e.getSource().equals(b_conn) == true) {
			if (b_conn.getLabel().equals("connect") == true) {

				jdbc = new HiveCommon(t_url.getText(), t_user.getText(),
						t_pwd.getText());

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
	}
}
