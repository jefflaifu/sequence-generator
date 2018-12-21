package com.jeff.sequence;

import com.jeff.sequence.dao.DBHelper;
import org.springframework.beans.factory.InitializingBean;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

/**
 *    流水号生成器，可以生成指定格式的流水号，并且最后一部分为序列。目前支持mysql数据库，并且可以支持
 *    多个序列生成并且互不影响。
 *   指定序列号格式时使用{@see java.text.MessageFormat}可以识别的格式，如{0,date,yyyyMMddHHmmss}{1}{2,number,000000}
 * 
 * @author 800P_zhangchi01
 *
 */
/**
 * @author 800P_zhangchi01
 *
 */
public class SequenceGenerator implements InitializingBean {

	/**
	 * java.text.MessageFormat的格式化文本
	 */
	private String pattern;

	private boolean dailyCutoff = false;

	/**
	 * 序列名称
	 */
	private String sequenceName;

	private final static int MAX_CACHE = 100000;
	
	private final static int MIN_CACHE = 100;

	/**
	 * 缓存大小，即缓存多少个序列值，最大MAX_CACHE，默认缓存100，减少数据库操作次数
	 */
	private int cache = MIN_CACHE;

	/**
	 * 默认步长为1
	 */
	private Integer step = 1;

	private DataSource dataSource;

	private DBHelper dbHelper;

	private ArrayBlockingQueue<Long> queue ;

	public void setDailyCutoff(boolean dailyCutoff) {
		this.dailyCutoff = dailyCutoff;
	}

	public void setPattern(String pattern) {
		this.pattern = pattern;
	}

	public void setSequenceName(String sequenceName) {
		this.sequenceName = sequenceName;
	}

	public void setCache(int cache) {
		if (cache > MAX_CACHE || cache < MIN_CACHE) {
			throw new IllegalArgumentException("cache must between " + MIN_CACHE + " and " + MAX_CACHE);
		} else {
			this.cache = cache;
		}
	}

	public void setStep(Integer step) {
		if ( step <= 0)
		{
			throw new IllegalArgumentException("step must not be 0 or negetive!");
		}
		this.step = step;
	}

	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		this.dbHelper = new DBHelper(dataSource, dailyCutoff);
		queue = new ArrayBlockingQueue<Long>(cache+1);

		// 建表
		dbHelper.createTable();
		// 建函数
		dbHelper.createFunction();
		// 新建序列
		dbHelper.createSequence(sequenceName, step);

		cache();
	}
	
	public void cache() throws Exception
	{
		Long end = next();
		Long start = end - cache * step;
		for (Long i = start; i <= end; i+=step)
		{
			queue.put(i);
		}
	}

	public String next(Object... params) throws Exception {
		Long seq = null;
		if (queue.size() == 0)
		{
			cache();
		}
		seq = queue.remove();
		List<Object> list = new ArrayList<>(Arrays.asList(params));
		list.add(seq);
		return MessageFormat.format(this.pattern, list.toArray());
	}

	private Long next() throws SQLException {
		return dbHelper.next(this.sequenceName, this.cache);
	}

}
