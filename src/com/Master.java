package com;
import java.io.IOException;
import org.apache.log4j.BasicConfigurator;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.async.DataCallbacker;
import com.async.ParentsCallbacker;
import com.async.StringCallbacker;

public class Master implements Watcher
{
	private Logger LOG = LoggerFactory.getLogger(this.getClass());
	private ZooKeeper zookeeper;
	private String hostPort;
	private String content = "Monkey";
	private StringCallbacker stringCallbacker = new StringCallbacker(this);
	private DataCallbacker dataCallbacker = new DataCallbacker(this);
	private ParentsCallbacker parentsCallbacker = new ParentsCallbacker(this);
 	public static boolean isLeader = false;
	
	/**
	 * Main Workplace
	 * @param args
	 */
	
	public static void main(String[] args){
		BasicConfigurator.configure();
		Master m = new Master(args[0]);
		try {
			m.startZookeeper();
			m.runMaster();
			m.bootStrap();
			m.getContent();			
			Thread.sleep(30000);			
			m.stopZookeeper();	//報錯原因 強制手動關閉
			
		} catch (Exception e) {			
			m.getLogger(e);		
		}	
	}
	
	public Master(String hostPort){
		this.hostPort = hostPort;
	}
	
	private void startZookeeper()throws IOException{
		this.zookeeper = new ZooKeeper(hostPort,15000,this);
	}
	
	/**
	 * To return the content from endpoint.
	 * @return String
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	private void getContent() throws KeeperException, InterruptedException{
		Stat stat = new Stat();
		byte[] data = zookeeper.getData("/Master", false, stat);	
		LOG.info("Data:{} , Exist:{}",new String(data),zookeeper.exists("/Master", this)); //檢查有無Master主節點 並且再次添加watcher 監視
	}
	
	private void stopZookeeper() throws Exception{		
		zookeeper.close();
	}
	
	/**
	 * @AsyncTypeEvent
	 * Try to be the main node.
	 */
	public void runMaster(){
		zookeeper.create("/Master", content.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE , CreateMode.EPHEMERAL,stringCallbacker,content.getBytes());
	}
	
	/**
	 * @AsyncTypeEvent
	 * Try to fetch data
	 */	
	public void checkMaster(){			
		zookeeper.getData("/Master", this, dataCallbacker, null);		
	}
	
	public void bootStrap() {
		createParents("/workers", new byte[0]);
		createParents("/assign", new byte[0]);
		createParents("/tasks", new byte[0]);
		createParents("/status", new byte[0]);
	}
	
	/**
	 * Building persistent node 
	 * @Main nodes
	 */
	public void createParents(String path,byte[] content)
	{
		zookeeper.create(path, content, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, parentsCallbacker, content);
	}
	
	/**
	 * Trigger Watcher
	 */
	@Override
	public void process(WatchedEvent e){
		System.out.println("Path："+e.getPath() + " Watcher-state："+e.getState()+" Watcher-type："+e.getType().toString());
	}
	
	final void getLogger(Exception e)
	{
		LOG.info("Exception: {}",e.getMessage());
	}	
}