package com.node;

import java.io.IOException;

import org.apache.log4j.BasicConfigurator;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.async.WorkersStatusStringCallbacker;
import com.async.WorkersStringCallBacker;

public class Workers implements Watcher {
	private Logger LOG = LoggerFactory.getLogger(this.getClass());
	private ZooKeeper zk;
	private String domain;
	private String status;
	private WorkersStringCallBacker wscb = new WorkersStringCallBacker(this);
	private WorkersStatusStringCallbacker wsscb = new WorkersStatusStringCallbacker(this);
	private String rdn = "Joe";

	/**
	 * Main Platform
	 * 
	 * @param args > Domain
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws IOException, InterruptedException {
		BasicConfigurator.configure();

		while (true) {
			Workers wk = new Workers(args[0]);
			wk.startZookeeper();
			wk.registry();
			Thread.sleep(Integer.MAX_VALUE);
			wk.stopZookeeper();
		}

	}

	/**
	 * Constructor
	 * 
	 * @param arg
	 */
	public Workers(String arg) {
		this.domain = arg;
	}

	/**
	 * Initialization ZK
	 * 
	 * @param arg
	 * @throws IOException
	 */
	public void startZookeeper() throws IOException {
		this.zk = new ZooKeeper(domain, 15000, this);
	}

	public void stopZookeeper() throws InterruptedException {
		zk.close();
	}

	public void registry() {

		zk.create("/workers/worker-" + rdn, "Idle".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, wscb, null);
	}

	/**
	 * When registry task successfully that will trigger this method.
	 * 
	 * @param status
	 */
	public void setStatus(String status) {
		this.status = status;
		updateStatus(status);

	}

	synchronized private void updateStatus(String status) {
		if (status == this.status) {
			zk.setData("/workers/worker-" + rdn, status.getBytes(), -1, wsscb, status);
		}
	}

	@Override
	public void process(WatchedEvent e) {

		/**
		 * Java assert is something like JUnit that is used for testing
		 */
		assert !"None".equals(e.getType().toString()) : "false";

		LOG.info("Path：{} , Watcher-state：{} , Watcher-type：{}", e.getPath(), e.getState(), e.getType().toString());
	}

}
