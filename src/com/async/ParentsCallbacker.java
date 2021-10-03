package com.async;

import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.node.Master;

public class ParentsCallbacker implements StringCallback {
	private Logger LOG = LoggerFactory.getLogger(this.getClass());
	private Master master;

	public ParentsCallbacker(Master master) {
		this.master = master;
	}

	@Override
	public void processResult(int resultCode, String path, Object ctx, String nodeName) {
		switch (Code.get(resultCode)) {
		case CONNECTIONLOSS:
			master.createParents(path, (byte[]) ctx); // 當訊號遺失重新發起一次"建立"目的確認是否建立成功
			break;
		case OK:
			LOG.info("Successfully built {}", path);
			break;
		case NODEEXISTS:
			LOG.info("Main Node exists already! : {}", path);
			break;
		default:
			LOG.error("something went wrong:", KeeperException.create(Code.get(resultCode)));
		}
	}
}