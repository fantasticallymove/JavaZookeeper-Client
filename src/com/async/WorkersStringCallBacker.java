package com.async;

import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.node.Workers;


public class WorkersStringCallBacker implements StringCallback{
	
	private Logger LOG = LoggerFactory.getLogger(this.getClass());
	private Workers wk;
	
	public WorkersStringCallBacker(Workers wk)
	{
		this.wk = wk;
	}
	
	@Override
	public void processResult(int resultCode, String path, Object ctx, String nodeName) {
		switch(Code.get(resultCode)){
			case CONNECTIONLOSS:
					wk.registry();;
					break;
			case OK:
					LOG.info("Code:{}, 成功建立工作任務，賦予任務狀態! Path-"+path,Code.get(resultCode));
					wk.setStatus("Busy");
					break;
			case NODEEXISTS:
					LOG.info("Code:{}, Node exists already! Path-"+ path,Code.get(resultCode));
					break;
			default:
					LOG.info("Something wrong Code:{}",KeeperException.create(Code.get(resultCode)));
		}
		
	}
}
