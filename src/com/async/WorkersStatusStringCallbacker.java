package com.async;

import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.node.Workers;

public class WorkersStatusStringCallbacker implements StatCallback{
	
	private Workers workers;
	private Logger LOG = LoggerFactory.getLogger(this.getClass());
	
	public WorkersStatusStringCallbacker(Workers workers)
	{
		this.workers = workers;
	}

	@Override
	public void processResult(int resultCode, String path, Object ctx, Stat stat) {
		switch(Code.get(resultCode))
		{
			case CONNECTIONLOSS:
				 workers.setStatus(new String((byte[])ctx));
				 break;
			case OK:
			     LOG.info("Setting Status is success!");
				 break;
		}
	}
}
