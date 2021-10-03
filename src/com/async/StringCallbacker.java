package com.async;

import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.node.Master;


public class StringCallbacker implements StringCallback
{
	private Logger LOG = LoggerFactory.getLogger(this.getClass());
	private Master master;
	
	public StringCallbacker(Master master)
	{
		this.master =  master;
	}
	
	@Override
	public void processResult(int resultCode, String path, Object ctx, String nodeName) {
		switch(Code.get(resultCode))
		{
			case CONNECTIONLOSS :			//發生網路異常.分區時處理方式
					master.checkMaster();	//zkCli再次執行檢查
					Master.isLeader = false;
					break;
			case OK: 						//成功建立時處理方式
					master.checkMaster();
					Master.isLeader = true;
					break;
			default:						//未建立成功時處理方式
					Master.isLeader = false;
		}
		LOG.info("Code: {} , Content: {} , NodeName: {}",Code.get(resultCode),new String((byte[])ctx),nodeName);
		System.out.println("I am "+(Master.isLeader ? "" : "not ")+"leader!");
	}
}
