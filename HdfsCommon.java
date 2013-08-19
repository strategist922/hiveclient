/**
 * 20130304 add by LIANGHS
 * Hadoop操作公用类
 */
package com.gw.hiveclient;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class HdfsCommon {

	private static HdfsCommon instance;

	private final Configuration conf;

	/**
	 * 操作完后是否关闭文件系统。 后台每个算法只用一个Configuration，不能关 一般调用需要关。
	 */
	private static boolean closeConf = false;

	private HdfsCommon(Configuration conf) {
		this.conf = conf;

		// conf.set("user", "hadoop");
		// conf.set("hadoop.job.ugi","hadoop,hadoop");

	}

	/**
	 * 操作后关闭文件系统
	 * 
	 * @return
	 */
	public static synchronized HdfsCommon getInstance() {
		if (instance == null) {
			instance = new HdfsCommon(new Configuration());
		}
		// closeConf=true;
		return instance;
	}

	/**
	 * 每个算法只用一个Configuration，任何操作后不能关文件系统
	 * 
	 * @param conf
	 * @return
	 */
	public static synchronized HdfsCommon getInstance(Configuration conf) {
		if (instance == null) {
			instance = new HdfsCommon(conf);
		}
		closeConf = false;
		return instance;
	}

	private FileSystem getFileSystem() throws IOException {
		FileSystem fs = FileSystem.get(conf);
		return fs;
	}

	/**
	 * 创建hadoop目录
	 * 
	 * @param hadoopPath
	 * @return
	 */
	public boolean mkdir(String hadoopPath) throws IOException {
		FileSystem hdfs = getFileSystem();
		Path path = new Path(hadoopPath);
		if (!hdfs.exists(path))
			hdfs.mkdirs(path);
		if (closeConf)
			hdfs.close();
		return true;
	}

	/**
	 * Hadoop文件追加内容
	 * 
	 * @param hadoopFile
	 * @param str
	 * @return
	 * @throws IOException
	 */
	public boolean appendFile(String hadoopFile, String str, String charset) throws IOException {
		if (str == null || hadoopFile == null)
			return false;

		FileSystem hdfs = getFileSystem();
		Path path = new Path(hadoopFile);
		FSDataOutputStream fout = null;
		if (!hdfs.exists(path))
			fout = hdfs.create(path);
		else
			fout = hdfs.append(path);
		BufferedWriter out = new BufferedWriter(new OutputStreamWriter(fout, charset));
		out.write(str);
		out.flush();
		out.close();

		if (closeConf)
			hdfs.close();

		return true;
	}

	/**
	 * 把字符串写到Hadoop文件中
	 * 
	 * @param hadoopFile
	 * @param str
	 * @return
	 * @throws IOException
	 */
	public boolean writeFile(String hadoopFile, String str, String charset) throws IOException {
		if (str == null || hadoopFile == null)
			return false;

		FileSystem fs = getFileSystem();
		Path path = new Path(hadoopFile);
		FSDataOutputStream fout = fs.create(path);
		BufferedWriter out = new BufferedWriter(new OutputStreamWriter(fout, charset));
		out.write(str);
		out.flush();
		out.close();
		if (closeConf)
			fs.close();
		return true;
	}

	/**
	 * HADOOP上的多文件合并: 第一个文件重命名，第二个及以后的文件append到第一个文件，原文件删除
	 * 
	 * @param fileList
	 *            文件清单
	 * @param targetFileName
	 *            目标文件名
	 * @param charset
	 *            字符集
	 * @throws IOException
	 */
	@SuppressWarnings("deprecation")
	public void combine(List<String> fileList, String targetFileName, String charset) throws IOException {
		FileSystem fs = getFileSystem();
		// 第一个文件重命名，第二个及以后的文件append到第一个文件。
		Path srcPath = new Path(fileList.get(0));
		Path path = new Path(targetFileName);
		fs.rename(srcPath, path);

		// Path path = new Path(targetFileName);
		// FSDataOutputStream fout = fs.create(path);
		FSDataOutputStream fout = fs.append(path);

		OutputStreamWriter outst = new OutputStreamWriter(fout, charset);
		char[] buffer = new char[16777216];
		int current = -1;
		// for (int i=0;i<fileList.size();i++){
		for (int i = 1; i < fileList.size(); i++) {
			Path inPath = new Path(fileList.get(i));
			if (!fs.exists(inPath))
				continue;
			FSDataInputStream iss = fs.open(inPath);
			InputStreamReader rst = new InputStreamReader(iss, charset);
			while ((current = rst.read(buffer, 0, 16777216)) != -1) {
				outst.write(buffer, 0, current);
				outst.flush();
			}
			rst.close();
			iss.close();
			fs.delete(inPath);
		}
		fout.flush();
		fout.close();
		if (closeConf)
			fs.close();
	}

	/**
	 * 读hadoop文件组成字符串
	 * 
	 * @param hadoopFile
	 *            Hadoop文件名
	 * @return
	 * @throws IOException
	 */
	public String readFile(String hadoopFile, String charset) throws IOException {
		return readFile(hadoopFile, -1, charset);
	}

	/**
	 * 读hadoop文件中的前count行数据组成字符串
	 * 
	 * @param hadoopFile
	 *            Hadoop文件名
	 * @param count
	 *            笔数
	 * @return
	 * @throws IOException
	 */
	public String readFile(String hadoopFile, int count, String charset) throws IOException {
		if (hadoopFile == null)
			return "";

		FileSystem hdfs = getFileSystem();
		Path path = new Path(hadoopFile);
		if (!hdfs.exists(path))
			return "";
		StringBuffer sb = new StringBuffer();
		FSDataInputStream iss = hdfs.open(path);
		BufferedReader inn = new BufferedReader(new InputStreamReader(iss, charset));
		int i = 0;
		String line;
		while ((line = inn.readLine()) != null) {
			sb.append(line + "\n");
			if (count < 0 || i < count)
				i++;
			else
				break;
		}
		inn.close();
		if (closeConf)
			hdfs.close();
		return sb.toString();
	}

	/**
	 * 判断hadoop上的文件是否存在
	 * 
	 * @param hadoopFileName
	 *            hadoop文件
	 * @return
	 */
	public boolean fileExists(String hadoopFileName) throws IOException {
		if (hadoopFileName == null)
			return false;
		FileSystem hdfs = getFileSystem();
		Path srcPath = new Path(hadoopFileName);
		return hdfs.exists(srcPath);
	}

	/**
	 * 判断是否为文件
	 * 
	 * @param hadoopFileName
	 *            hadoop文件/路径
	 * @return -1:文件或路径不存在。1：文件. 0:路径
	 */
	public int isFile(String hadoopFileName) throws IOException {
		if (hadoopFileName == null)
			return -1;
		FileSystem hdfs = getFileSystem();
		Path srcPath = new Path(hadoopFileName);
		if (!hdfs.exists(srcPath))
			return -1;
		else if (hdfs.isFile(srcPath))
			return 1;
		else
			return 0;
	}

	/**
	 * HADOOP上的文件重命名
	 * 
	 * @param oldFileName
	 *            旧文件
	 * @param newFileName
	 *            新文件
	 * @return
	 */
	public boolean renameFile(String oldFileName, String newFileName) throws IOException {
		FileSystem hdfs = getFileSystem();
		Path srcPath = new Path(oldFileName);
		Path dst = new Path(newFileName);
		return hdfs.rename(srcPath, dst);
	}

	/**
	 * 取文件个数(所有类型，但不包括目录)(文件名不包括路径)
	 * 
	 * @param hadoopPath
	 *            hadoop目录
	 * @return
	 */
	public int getFileNumb(String hadoopPath) throws IOException {
		List<String> list = getFileList(hadoopPath, 0, "", 0);
		if (list.size() == 0)
			return 0;
		else
			return list.size();
	}

	/**
	 * 取文件列表(所有类型，但不包括目录)(文件名不包括路径)，文件名有作排序
	 * 
	 * @param hadoopPath
	 *            hadoop目录
	 * @return
	 */
	public List<String> getFileList(String hadoopPath) throws IOException {
		List<String> list = getFileList(hadoopPath, 0, "", 0);
		Collections.sort(list);
		return list;
	}

	/**
	 * 取文件列表(所有类型，但不包括目录)(文件名包括路径)
	 * 
	 * @param hadoopPathhadoop目录
	 * @return
	 */
	public List<String> getFileList2(String hadoopPath) throws IOException {
		return getFileList(hadoopPath, 0, "", 1);
	}

	/**
	 * 取文件列表(文件名不包括路径)
	 * 
	 * @param hadoopPath
	 *            hadoop目录
	 * @param fileType
	 *            文件类型(扩展名) //20111203 add by LIANGHS
	 * @return
	 */
	public List<String> getFileList(String hadoopPath, String fileType) throws IOException {
		return getFileList(hadoopPath, 1, fileType, 0);
	}

	/**
	 * 取文件列表，不包括目录
	 * 
	 * @param hadoopPathhadoop的目录
	 * @param filterType
	 *            过虑类型：0:不过滤,1:过滤扩展名，2:过滤文件名头,3:全Like
	 * @param filterStr
	 *            过滤字符串
	 * @param returnType
	 *            返回的类型：0: 只有文件名；1:包括完整路径的文件名
	 * @return 找到的文件名清单，并且按字符ASSIC排好序
	 * @throws IOException
	 */
	public List<String> getFileList(String hadoopPath, int filterType, String filterStr, int returnType) throws IOException {
		if (hadoopPath == null)
			return null;
		while (hadoopPath.endsWith("/"))
			hadoopPath = hadoopPath.substring(0, hadoopPath.length() - 1);
		FileSystem hdfs = getFileSystem();
		Path srcPath = new Path(hadoopPath);
		FileStatus[] stat = hdfs.listStatus(srcPath);
		if (stat == null || stat.length == 0)
			return null;
		List<String> list = new ArrayList<String>();
		String ftype = filterStr.toLowerCase();
		for (int i = 0; i < stat.length; i++) {
			String fileName = stat[i].getPath().toString();
			fileName = fileName.substring(fileName.lastIndexOf("/") + 1);
			if (stat[i].isDir()) {
				List<String> listSub = getFileList(hadoopPath + "/" + fileName, filterType, filterStr, returnType);
				if (listSub != null) {
					for (int j = 0; j < listSub.size(); j++)
						list.add(listSub.get(j));
				}
			} else {
				String fn = fileName.toLowerCase();
				if (returnType == 1)
					fileName = hadoopPath + "/" + fileName;

				if (filterType == 1) {// 1:过滤扩展名
					if (fn.endsWith(ftype)) {
						list.add(fileName);
					}
				} else if (filterType == 2) {// 2:过滤文件名头
					if (fn.startsWith(ftype)) {
						list.add(fileName);
					}
				} else if (filterType == 2) {// 3:全Like
					if (fn.indexOf(ftype) > -1) {
						list.add(fileName);
					}
				} else
					list.add(fileName);
			}
		}
		Collections.sort(list);
		return list;
	}

	/**
	 * 取目录列表
	 * 
	 * @param hadoopPath
	 *            HADOOP的目录
	 * @return 找到的目录清单，并且按字符ASSIC排好序
	 * @throws IOException
	 */
	public List<String> getDirectoryList(String hadoopPath) throws IOException {
		if (hadoopPath == null)
			return null;
		while (hadoopPath.endsWith("/"))
			hadoopPath = hadoopPath.substring(0, hadoopPath.length() - 1);
		FileSystem hdfs = getFileSystem();
		Path srcPath = new Path(hadoopPath);
		FileStatus[] stat = hdfs.listStatus(srcPath);
		if (stat == null || stat.length == 0)
			return null;
		List<String> list = new ArrayList<String>();
		for (int i = 0; i < stat.length; i++) {
			String fileName = stat[i].getPath().toString();
			fileName = fileName.substring(fileName.lastIndexOf("/") + 1);
			if (stat[i].isDir()) {
				list.add(fileName);
			}
		}
		Collections.sort(list);
		return list;
	}

	/**
	 * 删除hadoop中的文件或者目录（目录可为不空）
	 * 
	 * @param hadoopFileName
	 * @return
	 */
	@SuppressWarnings("deprecation")
	public boolean deleteHadoopFile(String hadoopFileName) throws IOException {
		if (hadoopFileName == null)
			return true;
		FileSystem hdfs = getFileSystem();
		Path srcPath = new Path(hadoopFileName);
		return hdfs.delete(srcPath);
	}

	/**
	 * 判断hadoop文件是否比本地文件新。
	 * 
	 * @param localFileName
	 * @param hadoopFileName
	 * @return
	 * @throws IOException
	 */
	@SuppressWarnings("deprecation")
	public boolean isNewFile(String localFileName, String hadoopFileName) throws IOException {
		if (localFileName == null || hadoopFileName == null)
			return false;
		File file = new File(localFileName);
		if (!file.exists())
			return true;
		FileSystem hdfs = getFileSystem();
		Path srcPath = new Path(hadoopFileName);
		if (!hdfs.exists(srcPath))
			return false;
		if (file.length() != hdfs.getLength(srcPath))
			return true;
		FileStatus fst = hdfs.getFileStatus(srcPath);
		if (file.lastModified() < fst.getModificationTime())
			return true;
		return false;
	}

	/**
	 * 取hadoop上的文件大小
	 * 
	 * @param hadoopFileName
	 * @return
	 * @throws IOException
	 */
	@SuppressWarnings("deprecation")
	public long getHadoopFileSize(String hadoopFileName) throws IOException {
		if (hadoopFileName == null)
			return 0;
		FileSystem hdfs = getFileSystem();
		Path srcPath = new Path(hadoopFileName);
		return hdfs.getLength(srcPath);
	}

	/**
	 * 从HADOOP下载(复制的方式)一个文件到本地
	 * 
	 * @param hadoopFileName
	 *            HADOOP文件名(含路径)
	 * @param localFileName
	 *            本地的文件名(含路径)
	 * @return 下载成功后本地的文件名(含路径)
	 */
	public String downloadFile(String hadoopFileName, String localFileName) throws IOException {
		return downloadFile(false, hadoopFileName, localFileName);
	}

	/**
	 * 从HADOOP下载(复制 or move)一个文件到本地
	 * 
	 * @param deleteFlag
	 *            true/false,true:删除源文件，即移动
	 * @param hadoopFileName
	 *            HADOOP文件名(含路径)
	 * @param localFileName
	 *            本地的文件名(含路径)
	 * @return 下载成功后本地的文件名(含路径)
	 */
	public String downloadFile(boolean deleteFlag, String hadoopFileName, String localFileName) throws IOException {
		String localPath = localFileName.substring(0, localFileName.lastIndexOf("/") + 1);
		FileCommon.localMkdir(localPath);
		FileSystem hdfs = getFileSystem();
		Path srcPath = new Path(hadoopFileName);
		if (!hdfs.exists(srcPath)) {
			hdfs.close();
			throw new IOException("file not exists: " + hadoopFileName);
		}
		File dstFile = new File(localFileName);
		FileUtil.copy(hdfs, srcPath, dstFile, deleteFlag, conf);
		if (closeConf)
			hdfs.close();
		return localFileName;
	}

	/**
	 * 从HADOOP下载(复制)文件到本地
	 * 
	 * @param hadoopFileNamen
	 *            HADOOP文件(含路径)
	 * @param localPath
	 *            本地目录
	 * @return 下载成功后本地的文件名(含路径)
	 */
	public String downloadFile2(String hadoopFileName, String localPath) throws IOException {
		return downloadFile2(false, hadoopFileName, localPath);
	}

	/**
	 * 从HADOOP下载(复制 or move)文件到本地
	 * 
	 * @param deleteFlag
	 *            true/false,true:删除源文件，即移动
	 * @param hadoopFileName
	 *            HADOOP文件(含路径)
	 * @param localPath
	 *            本地目录
	 * @return 下载成功后本地的文件名(含路径)
	 */
	public String downloadFile2(boolean deleteFlag, String hadoopFileName, String localPath) throws IOException {
		String localFileName = localPath + hadoopFileName.substring(hadoopFileName.lastIndexOf("/") + 1);
		return downloadFile(false, hadoopFileName, localFileName);
	}

	/**
	 * 从HADOOP下载(复制)目录(or 文件)到本地目录
	 * 
	 * @param hadoopPath
	 *            目录(or 文件)(含路径)
	 * @param localPath
	 *            本地目录
	 * @return true：成功; false:失败
	 */
	public boolean downloadFiles(String hadoopPath, String localPath) throws IOException {
		return downloadFiles(false, hadoopPath, localPath);
	}

	/**
	 * 从HADOOP下载(复制 or 移动)目录(or 文件)到本地目录
	 * 
	 * @param deleteFlag
	 *            true/false,true:删除源文件，即移动
	 * @param hadoopPath
	 *            目录(or 文件)
	 * @param localPath
	 *            本地目录
	 * @return true：成功; false:失败
	 */
	@SuppressWarnings("deprecation")
	public boolean downloadFiles(boolean deleteFlag, String hadoopPath, String localPath) throws IOException {
		if (conf == null || hadoopPath == null || localPath == null)
			return false;

		FileSystem hdfs = getFileSystem();
		Path srcPath = new Path(hadoopPath);
		if (!hdfs.exists(srcPath)) {
			hdfs.close();
			return false;
		}
		// 判断是否为目录，若为目录，则递归调用，否则直接复制
		if (hdfs.isDirectory(srcPath)) {
			FileStatus[] sta = hdfs.listStatus(srcPath);
			if (sta == null || sta.length < 1) {
				hdfs.close();
				return false;
			}
			for (int i = 0; i < sta.length; i++) {
				if (sta[i].isDir()) {
					String newHadoopPath = sta[i].getPath().toString();
					String newLocalPath = localPath + "/" + sta[i].getPath().getName();
					FileCommon.localMkdir(newLocalPath);
					downloadFiles(deleteFlag, newHadoopPath, newLocalPath);
				} else {
					String hadoopFileName = sta[i].getPath().toString();
					String localFileName = localPath + "/" + sta[i].getPath().getName();
					downloadFile(deleteFlag, hadoopFileName, localFileName);
				}
			}
		} else {
			FileCommon.localMkdir(localPath);
			downloadFile2(deleteFlag, hadoopPath, localPath);
		}
		if (closeConf)
			hdfs.close();
		return true;
	}

	/**
	 * 从HADOOP下载(复制)文件到本地
	 * 
	 * @param fileList
	 *            HADOOP文件列表(含路径)
	 * @param localPath
	 *            本地目录
	 * @param deleteFlag
	 *            true/false,true:删除源文件，即移动
	 * @return 下载成功后本地的文件清单(含路径)
	 */
	public List<String> downloadFiles2(List<String> fileList, String localPath) throws IOException {
		return this.downloadFiles2(fileList, localPath, false);
	}

	/**
	 * 从HADOOP下载(复制 or 移动)文件到本地
	 * 
	 * @param fileList
	 *            HADOOP文件列表(含路径)
	 * @param localPath
	 *            本地目录
	 * @param deleteFlag
	 *            true/false,true:删除源文件，即移动
	 * @return 下载成功后本地的文件清单(含路径)
	 */
	public List<String> downloadFiles2(List<String> fileList, String localPath, boolean deleteFlag) throws IOException {
		if (fileList == null || localPath == null)
			return null;
		FileCommon.localMkdir(localPath);
		List<String> list = new ArrayList<String>();
		FileSystem hdfs = getFileSystem();
		for (int i = 0; i < fileList.size(); i++) {
			String hadoopFileName = fileList.get(i);// HADOOP文件
			// 本地文件名
			String localFileName = localPath + hadoopFileName.substring(hadoopFileName.lastIndexOf("/") + 1);
			Path srcPath = new Path(hadoopFileName);
			if (!hdfs.exists(srcPath)) {
				hdfs.close();
				throw new IOException("file not exists: " + hadoopFileName);
			}
			File dstFile = new File(localFileName);
			FileUtil.copy(hdfs, srcPath, dstFile, deleteFlag, conf);// 下载
			list.add(localFileName);
		}
		if (closeConf)
			hdfs.close();
		return list;
	}

	/**
	 * 从本地上传(复制)一个文件到HADOOP
	 * 
	 * @param localFileName
	 *            本地文件名(含路径)
	 * @param hadoopFileName
	 *            HADOOP文件(含路径)
	 * @return 上传成功后HADOOP的文件名(含路径)
	 */
	public String uploadFile(String localFileName, String hadoopFileName) throws IOException {
		return uploadFile(false, localFileName, hadoopFileName);
	}

	/**
	 * 从本地上传(复制 or move)一个文件到HADOOP
	 * 
	 * @param deleteFlag
	 *            true/false,true:删除源文件，即移动
	 * @param localFileName
	 *            本地文件名(含路径)
	 * @param hadoopFileName
	 *            HADOOP文件(含路径)
	 * @return 上传成功后HADOOP的文件名(含路径)
	 */
	@SuppressWarnings("deprecation")
	public String uploadFile(boolean deleteFlag, String localFileName, String hadoopFileName) throws IOException {
		File lfile = new File(localFileName);
		if (!lfile.exists()) {
			return "";
		}
		FileSystem hdfs = getFileSystem();
		File srcFile = new File(localFileName);
		Path dstPath = new Path(hadoopFileName);
		FileUtil.fullyDelete(hdfs, dstPath);
		FileUtil.copy(srcFile, hdfs, dstPath, deleteFlag, conf);
		// 加第一个参数true时为移动文件
		if (closeConf)
			hdfs.close();
		return hadoopFileName;
	}

	/**
	 * 从本地上传(复制)一个文件到HADOOP
	 * 
	 * @param localFileName
	 *            本地文件名(含路径)
	 * @param hadoopPath
	 *            HADOOP目录
	 * @return 上传成功后HADOOP的文件名(含路径)
	 */
	public String uploadFile2(String localFileName, String hadoopPath) throws IOException {
		return uploadFile2(false, localFileName, hadoopPath);
	}

	/**
	 * 从本地上传(复制 or move)一个文件到HADOOP
	 * 
	 * @param deleteFlag
	 *            true/false,true:删除源文件，即移动
	 * @param localFileName
	 *            本地文件名(含路径)
	 * @param hadoopPath
	 *            HADOOP目录
	 * @return 上传成功后HADOOP的文件名(含路径)
	 */
	public String uploadFile2(boolean deleteFlag, String localFileName, String hadoopPath) throws IOException {
		String hadoopFile = hadoopPath + localFileName.substring(localFileName.lastIndexOf("/"));
		return uploadFile(deleteFlag, localFileName, hadoopFile);
	}

	/**
	 * 从本地上传(复制)一个目录(or 文件)到HADOOP
	 * 
	 * @param localPath
	 *            本地目录
	 * @param hadoopPath
	 *            HADOOP目录
	 * @return true：成功; false:失败
	 */
	public boolean uploadFiles(String localPath, String hadoopPath) throws IOException {
		return uploadFiles2(false, localPath, hadoopPath);
	}

	/**
	 * 从本地上传(复制or move)一个目录(or 文件)到HADOOP
	 * 
	 * @param deleteFlag
	 *            true/false,true:删除源文件，即移动
	 * @param localPath
	 *            本地目录
	 * @param hadoopPath
	 *            HADOOP目录
	 * @return true：成功; false:失败
	 */
	public boolean uploadFiles2(boolean deleteFlag, String localPath, String hadoopPath) throws IOException {
		if (conf == null || localPath == null || hadoopPath == null)
			return false;
		File lfile = new File(localPath);
		if (!lfile.exists()) {
			return false;
		}
		if (lfile.isDirectory()) {// 本地是目录，用循环递归的方式来取得文件清单
			String fileArr[] = lfile.list();// 列出本地文件清单
			for (int i = 0; i < fileArr.length; i++) {
				String newLocalPath = localPath + "/" + fileArr[i];
				String newHadoopPath = hadoopPath + "/" + fileArr[i];
				File newfile = new File(newLocalPath);
				if (newfile.isFile())
					uploadFile(deleteFlag, newLocalPath, newHadoopPath);
				else
					uploadFiles2(deleteFlag, newLocalPath, newHadoopPath);
			}
		} else {
			uploadFile2(deleteFlag, localPath, hadoopPath);
		}
		return true;
	}
}
