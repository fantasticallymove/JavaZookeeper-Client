package com.node;

import java.io.IOException;

import org.apache.log4j.BasicConfigurator;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.async.ChildrenCallbacker;
import com.async.DataCallbacker;
import com.async.ParentsCallbacker;
import com.async.RootListCache;
import com.async.StringCallbacker;

public class Master implements Watcher {

	private RootListCache<String> cache;
	private Logger LOG = LoggerFactory.getLogger(this.getClass());
	private ZooKeeper zookeeper;
	private String hostPort;
	private String content = "Monkey";
	private StringCallbacker stringCallbacker = new StringCallbacker(this);
	private DataCallbacker dataCallbacker = new DataCallbacker(this);
	private ParentsCallbacker parentsCallbacker = new ParentsCallbacker(this);
	private ChildrenCallbacker childrenCallbacker = new ChildrenCallbacker(this, cache);
	public static boolean isLeader = false;

	/**
	 * Main Workplace
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		BasicConfigurator.configure();
		Master m = new Master(args[0]);
		try {
			m.startZookeeper();
			m.runMaster();
			m.bootStrap();
			m.getContent();
			Thread.sleep(Integer.MAX_VALUE);
			m.stopZookeeper(); // 報錯原因 強制手動關閉

		} catch (Exception e) {
			m.getLogger(e);
		}
	}

	public Master(String hostPort) {
		this.hostPort = hostPort;
	}

	private void startZookeeper() throws IOException {
		this.zookeeper = new ZooKeeper(hostPort, 15000, this);
	}

	/**
	 * To return the content from endpoint.
	 */
	private void getContent() {
		zookeeper.getData("/Master", this, dataCallbacker, content.getBytes());
	}

	/**
	 * To correctly close main process
	 * 
	 * @throws Exception
	 */
	private void stopZookeeper() throws Exception {
		zookeeper.close();
	}

	/**
	 * @AsyncTypeEvent Try to be the main node.
	 */
	public void runMaster() {
		zookeeper.create("/Master", content.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,
				stringCallbacker, content.getBytes());
	}

	/**
	 * @AsyncTypeEvent Try to fetch data
	 */
	public void checkMaster() {
		zookeeper.getData("/Master", this, dataCallbacker, null);
	}

	/**
	 * The list of initialization of mainNodes
	 */
	public void bootStrap() {
		createParents("/workers", new byte[0]);
		createParents("/assign", new byte[0]);
		createParents("/tasks", new byte[0]);
		createParents("/status", new byte[0]);
	}

	/**
	 * Getting WorkerList
	 */
	public void getWorkerList() {
		zookeeper.getChildren("/", this, childrenCallbacker, null);
	}

	/**
	 * Building persistent node
	 * 
	 * @Main nodes
	 * @AsyncTypeEvent
	 */
	public void createParents(String path, byte[] content) {
		zookeeper.create(path, content, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, parentsCallbacker, content);
	}

	/**
	 * Trigger Watcher
	 */
	@Override
	public void process(WatchedEvent e) {
		LOG.info("Path：{} , Watcher-state：{} , Watcher-type：{}", e.getPath(), e.getState(), e.getType().toString());
		try {
			/**
			 * Getting root list
			 */
			zookeeper.getChildren("/", this, childrenCallbacker, null);

			/**
			 * Getting worker list
			 */
			Object[] workerlist = zookeeper.getChildren("/workers", this).toArray();

			for (Object ele : workerlist) {
				LOG.info("Now worker list - {}", ele);
			}

			byte[] testMessageInCallback = "測試".getBytes(); // put message to callback

			zookeeper.getData("/Master", this, dataCallbacker, testMessageInCallback);

		} catch (KeeperException | InterruptedException e1) {
			e1.printStackTrace();
		}
	}

	final void getLogger(Exception e) {
		LOG.info("Exception: {}", e.getMessage());
	}
}