package spark.dao;

import java.util.List;

import spark.domain.CarTrack;

public interface ICarTrackDAO {
	
	/**
	 * 批量插入车辆轨迹信息
	 * @param carTracks
	 */
	void insertBatchCarTrack(List<CarTrack> carTracks);
}
