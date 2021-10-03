package com.async;

import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.node.Master;



public class DataCallbacker implements DataCallback
{
	private Logger LOG = LoggerFactory.getLogger(this.getClass());
	private Master master;
	
	public DataCallbacker(Master master)
	{
		this.master = master;
	}
	
	@Override
	public void processResult(int resultCode, String path, Object ctx, byte[] data, Stat stat) {
		
		LOG.info("Data: {} , {}",new String(data),stat);		//觀測數據
		
		switch(Code.get(resultCode))
		{			
			case CONNECTIONLOSS :					//發生網路分區 連線中斷 就在嘗試檢查一次 recursive
						master.checkMaster();
						break;
			case NONODE :							//當確認無資料表示無節點，執行創建
						master.runMaster();
						break;
			default:
				LOG.info("Code :{} , Node exists already!",Code.get(resultCode));
		}	
		
	}
}
