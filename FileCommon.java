/**
 * 本地文件操作类
 */
package com.gw.hiveclient;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class FileCommon {

	/**
	 * 取目录下的某种类型的文件清单
	 * 
	 * @param path
	 *            路径
	 * @param extName
	 *            文件扩展名
	 * @return
	 */
	@SuppressWarnings("null")
	public static List<String> getFiles(String path, String extName) {
		File dir = new File(path);
		File[] children = dir.listFiles();
		List<String> list = new ArrayList<String>();
		if (children == null && children.length == 0)
			return list;
		for (int i = 0; i < children.length; i++) {
			File file = children[i];
			if (!file.isFile())
				continue;
			String fileName = file.getName();
			if ("*".equals(extName) || fileName.toLowerCase().endsWith(extName.toLowerCase()))
				list.add(fileName);
		}
		Collections.sort(list);
		return list;
	}

	/**
	 * 判断本地文件是否存在
	 * 
	 * @param fileName
	 *            文件存
	 * @return
	 */
	public static boolean fileExists(String fileName) {
		if (fileName == null)
			return false;
		File fl = new File(fileName);
		return fl.exists();
	}

	/**
	 * 删除文件
	 * 
	 * @param fileName
	 */
	public static boolean deleteFile(String fileName) {
		if (fileName == null)
			return true;
		File fl = new File(fileName);
		return fl.delete();
	}

	/**
	 * 创建本地目录，若目录已经存在，则不创建。
	 * 
	 * @param path
	 *            目录名
	 * @return 目录存在或者创建成功时返回true，否则为false
	 */
	public static boolean localMkdir(String path) {
		if (path == null)
			return false;
		File fl = new File(path);
		if (!fl.exists())
			return fl.mkdir();
		else
			return true;
	}

	/**
	 * 写本地文件，并
	 * 
	 * @param fileName
	 *            文件名，包含完整的路径
	 * @param str
	 *            文件内容
	 * @param charset
	 *            文件的字符集
	 * @return
	 * @throws IOException
	 */
	public static boolean writeFile(String fileName, String str, String charset) throws IOException {
		FileOutputStream fout = new FileOutputStream(fileName);
		Writer write = new OutputStreamWriter(fout, charset);
		write.write(str);
		write.close();
		fout.close();
		return true;
	}

	/**
	 * 追加本地文件，并
	 * 
	 * @param fileName
	 *            文件名，包含完整的路径
	 * @param str
	 *            文件内容
	 * @return
	 * @throws IOException
	 */
	public static boolean appendFile(String fileName, String str) throws IOException {
		BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName, true), "utf-8"));
		out.write(str);
		out.close();
		return true;
		// FileWriter write = new FileWriter(fileName, true,PlatformPar.charset);
		// write.write(str);
		// write.close();
	}

	/**
	 * 读本地文件，并
	 * 
	 * @param fileName
	 *            文件名，包含完整的路径
	 * @return
	 * @throws IOException
	 */
	public static String readFile(String fileName) throws IOException {
		FileInputStream fin = new FileInputStream(fileName);
		BufferedReader inn = new BufferedReader(new InputStreamReader(fin, "utf-8"));
		String line;
		StringBuffer sb = new StringBuffer();
		while ((line = inn.readLine()) != null) {
			sb.append(line + "\n");
		}
		inn.close();
		fin.close();
		return sb.toString();
	}

	/**
	 * 创建文件
	 * 
	 * @param fileName
	 * @return boolean
	 * @throws IOException
	 */
	public static boolean createFile(String fileName, boolean overwrite) throws IOException {
		File file = new File(fileName);
		if (file.exists()) {
			if (!overwrite) {
				return false;
			}
			file.delete();
		}
		return file.createNewFile();
	}

	/**
	 * 生成一个本地临时目录
	 * 
	 * @param
	 * @return 本地临时路径
	 */
	public static String getLocalOutputPath(String path) {
		// 取本地的临时目录，用于存放本地临时文件使用。
		// String path = PlatformPar.localTempPath + PlatformPar.tmpDirHead + DateCommon.getCurrentDate1("ddHHmmssSSS");
		Random rand = new Random();
		path = path + rand.nextInt(100) + "/";
		File dir = new File(path);
		if (!dir.mkdirs())
			return "";
		return path;
	}

	/**
	 * 删除某个文件夹下的所有文件夹和文件
	 * 
	 * @param delpath
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @return boolean
	 */
	public static boolean deletePath(String delpath) throws Exception {
		try {
			File file = new File(delpath);
			// 当且仅当此抽象路径名表示的文件存在且 是一个目录时，返回 true
			if (!file.isDirectory()) {
				file.delete();
			} else if (file.isDirectory()) {
				String[] filelist = file.list();
				for (int i = 0; i < filelist.length; i++) {
					File delfile = new File(delpath + "/" + filelist[i]);
					if (!delfile.isDirectory()) {
						delfile.delete();
					} else if (delfile.isDirectory()) {
						deletePath(delpath + "/" + filelist[i]);
					}
				}
				file.delete();
			}
		} catch (FileNotFoundException e) {
			throw e;
		}
		return true;
	}
}
