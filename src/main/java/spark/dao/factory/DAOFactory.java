package spark.dao.factory;

import spark.dao.IAreaDao;
import spark.dao.ICarTrackDAO;
import spark.dao.IMonitorDAO;
import spark.dao.IRandomExtractDAO;
import spark.dao.ITaskDAO;
import spark.dao.IWithTheCarDAO;
import spark.dao.impl.AreaDaoImpl;
import spark.dao.impl.CarTrackDAOImpl;
import spark.dao.impl.MonitorDAOImpl;
import spark.dao.impl.RandomExtractDAOImpl;
import spark.dao.impl.TaskDAOImpl;
import spark.dao.impl.WithTheCarDAOImpl;

/**
 * DAO工厂类
 *
 */
public class DAOFactory {
	
	
	public static ITaskDAO getTaskDAO(){
		return new TaskDAOImpl();
	}
	
	public static IMonitorDAO getMonitorDAO(){
		return new MonitorDAOImpl();
	}
	
	public static IRandomExtractDAO getRandomExtractDAO(){
		return new RandomExtractDAOImpl();
	}
	
	public static ICarTrackDAO getCarTrackDAO(){
		return new CarTrackDAOImpl();
	}
	
	public static IWithTheCarDAO getWithTheCarDAO(){
		return new WithTheCarDAOImpl();
	}

	public static IAreaDao getAreaDao() {
		return  new AreaDaoImpl();
		
	}
}
