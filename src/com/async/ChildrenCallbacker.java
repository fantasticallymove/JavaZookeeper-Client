package com.async;

import java.util.List;

import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.node.Master;

public class ChildrenCallbacker implements ChildrenCallback {

	private RootListCache<String> cache;
	private Logger LOG = LoggerFactory.getLogger(this.getClass());
	private Master master;

	public ChildrenCallbacker(Master master,RootListCache<String>cache) {
		this.master = master;
		this.cache = cache;
	}

	@Override
	public void processResult(int code, String path, Object ctx, List<String> childrenList) {
		switch (Code.get(code)) {
		/**
		 * 當失敗時再次嘗試取得workerList
		 */
		case CONNECTIONLOSS:
			LOG.info(" 《{}》 ChildrenCallbacker task failed, try to CONNECTION", Code.CONNECTIONLOSS);
			master.getWorkerList();
			break;
		/**
		 * 當OK時將列表快取給其他模組使用 並且呈現每個name
		 */
		case OK:
			cacheList(childrenList);
			LOG.info(" 《{}》 ChildrenCallbacker task is success, try to cache !", Code.OK);
			break;
		/**
		 * 取得完全失敗
		 */
		default:
			LOG.info(" 《{}》 ChildrenCallbacker task failed and can't reconnect", Code.get(code));
		}
	}

	/**
	 * 快取整個表單下來
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void cacheList(List<String> cacheList) {
		try {

			/**
			 * 當本地 cache 還沒被初始化
			 * 使用根目錄快取類
			 */
			if (cache == null) {
				LOG.info("快取為空,執行初始化");
				cache = new RootListCache(cacheList);
			}
			else{
				LOG.info("快取非空,執行快取任務");
				cache.removeAndSet(cacheList);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
