package com.node;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Random;

import org.apache.log4j.BasicConfigurator;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.OpResult.CreateResult;
import org.apache.zookeeper.Transaction;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.async.WorkersStatusStringCallbacker;
import com.async.WorkersStringCallBacker;

public class Workers implements Watcher {
	private Logger LOG = LoggerFactory.getLogger(this.getClass());
    public ZooKeeper zk;
	private String domain;
	private String status;
	private WorkersStringCallBacker workersStringCallBacker = new WorkersStringCallBacker(this);
	private WorkersStatusStringCallbacker wsscb = new WorkersStatusStringCallbacker(this);
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddhhmmss");
	private String rdn = sdf.format(new Date(System.currentTimeMillis()));

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

		zk.create("/workers/worker-" + rdn, "Idle".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,
				workersStringCallBacker, null);
	}

	/**
	 * When registry task successfully that will trigger this method.
	 * workersStringCallBacker will trigger this method
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
	
	/**
	 * 多重任務結構範例
	 * 含返回判斷
	 */
	@Override
	public void process(WatchedEvent e) {

		/**
		 * Java assert is something like JUnit that is used for testing
		 */
		assert !"None".equals(e.getType().toString()) : "false";

		LOG.info("Path：{} , Watcher-state：{} , Watcher-type：{}", e.getPath(), e.getState(), e.getType().toString());
		try {
			
			/**
			 * 原子型
			 * 多任務封裝
			 * Transaction
			 */
			Boolean random = new Random().nextBoolean();
			Transaction tran = zk.transaction();
			tran.create("/status/status-"+rdn+random+"1", random.toString().getBytes(),Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			tran.create("/status/status-"+rdn+random+"2", random.toString().getBytes(),Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			tran.create("/status/status-"+rdn+random+"3", random.toString().getBytes(),Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			List<OpResult> results = tran.commit(); //代替執行zk multi method
			for(OpResult ele : results)
			{
				if(ele instanceof OpResult.CreateResult) //個別轉型
				{
					int code = ele.getType();
					switch(code)
					 {
					 	case OpCode.create:
					 		 LOG.info("多重任務結果 - 成功 {}",((CreateResult)ele).getPath());
					 		 break;
					 	case OpCode.error:
					 		 LOG.info("多重任務結果 - 失敗 {}",((CreateResult)ele).getPath());
					 		 break;
					 }
					
				}
				
			}
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (KeeperException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	
	Op deleteZnode(String z)
	{
		return Op.delete(z, -1);
	}
	

}
