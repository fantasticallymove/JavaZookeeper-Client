package com.async;

import java.util.Iterator;
import java.util.List;

/**
 * 給一次性工作任務使用的快取列表
 * @author user
 * @param <E>
 */
public class WorkersCache <E>{
	List<E> data;
	
	public WorkersCache(List<E> cacheDataList)
	{
		this.data = cacheDataList;
	}
	
	/**
	 * 刷新cache
	 * @throws Exception 
	 */
	public List<E> removeAndSet(List<E> cacheList) throws Exception
	{	
		/**
		 * 假如本地快取已經被初始化
		 */
		if(data!=null)
		{
			Iterator<E> dataIter = data.iterator();
			while(dataIter.hasNext())
			{
				E ele = dataIter.next();
				/**
				 * 假如傳進來的列表內容已經沒有再次出現了
				 * 就移除本地快取，代表該工作內容已經完成。
				 */
				if(!cacheList.contains(ele))
				{
					data.remove(ele);
				}
							
			}
		}
		else
		{
			throw new Exception("Cache isn't initialized");
		}
		
		
		return data;
	}
}
