package com.async;

import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 快取整個根目錄方便各class追蹤使用整個列表
 * 
 * @author user
 *
 * @param <E>
 */
public class RootListCache<E> {

	private Logger LOG = LoggerFactory.getLogger(getClass());

	private List<E> data;

	public RootListCache(List<E> cacheDataList) {
		this.data = cacheDataList;
		for (int count = 0; count < data.size(); count++) {
			LOG.info("快取本地服務列表根目錄 排序:{}- URL:{}", count + 1, data.get(count));
		}
	}

	/**
	 * 刷新cache
	 * 
	 * @throws Exception
	 */
	public List<E> removeAndSet(List<E> cacheList) throws Exception {
		/**
		 * 假如本地快取已經被初始化
		 */
		if (data != null) {

			/**
			 * 對data 本地上鎖 保證一致姓
			 */
			synchronized (data) {
				for (E e : cacheList) {
					/**
					 * 假如傳進來的列表內容沒有出現過 就納入本地快取
					 */
					if (!data.contains(e)) {
						data.add(e);
					}
				}

				/**
				 * 比對一輪 假如當前表單沒有快取內的就移除快取內的
				 */
				Iterator<E> dataList = data.iterator();

				while (dataList.hasNext()) {
					/**
					 * 假如傳進來的列表內容已經沒有快取過的，就移除快取
					 */
					E ele = dataList.next();
					if (!cacheList.contains(ele)) {
						dataList.remove(); // 直接使用Iterator remove是因為有比對原參考問題 直接使用本地remove會導致
											// 迭代器的modCount和expectedModCount的值不一致。
											// java.util.ConcurrentModificationException 採坑
											// 多執行緒中更容易出現該異常，當你在一個執行緒中對一資料集合進行遍歷，正趕上另外一個執行緒對該資料集合進行增刪操作
					}
				}
			}

		} else {
			throw new Exception("Cache isn't initialized");
		}

		/**
		 * 執行快取LOG顯示
		 */
		for (int count = 0; count < data.size(); count++) {
			LOG.info("快取本地服務列表根目錄 排序:{}- URL:{}", count + 1, data.get(count));
		}

		return data;
	}
}
