package spark.skynet;

import MockData.MockData;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Optional;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;
import spark.conf.ConfigurationManager;
import spark.constant.Constants;
import spark.dao.IMonitorDAO;
import spark.dao.ITaskDAO;
import spark.dao.factory.DAOFactory;
import spark.domain.MonitorState;
import spark.domain.Task;
import spark.domain.TopNMonitor2CarCount;
import spark.domain.TopNMonitorDetailInfo;
import spark.skynet.accumlator.MonitorAndCameraStateAccumulator;
import spark.skynet.sort4key.SpeedSortKey;
import utils.ParamUtils;
import utils.SparkUtils;
import utils.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 【卡口流量监控模块】
 *	1.检测卡扣状态
 *  2.获取车流排名前N的卡扣号
 *  3.数据库保存累加器5个状态（正常卡扣数，异常卡扣数，正常摄像头数，异常摄像头数，异常摄像头的详细信息）
 *  4.topN 卡口的车流量具体信息存库
 *  5.获取高速通过的TOPN卡扣
 *  6.获取车辆高速通过的TOPN卡扣，每一个卡扣中车辆速度最快的前10名
 *  7.区域碰撞分析
 *  8.卡扣碰撞分析
 *
 *  【作业提交】
 *  ./spark-submit  --master spark://mini:7077
 *  --class spark.skynet.MonitorFlowAnalyze
 *  --driver-class-path ../lib/mysql-connector-java-5.1.6.jar:../lib/fastjson-1.2.11.jar
 *  --jars ../lib/mysql-connector-java-5.1.6.jar,../lib/fastjson-1.2.11.jar
 *  ../lib/ProduceData2Hive.jar
 *  1
 */
public class MonitorFlowAnalyze {

    public static void main(String[] args) {

        //构建Spark运行时的环境参数
        SparkConf conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_SESSION);
        //指定Master
        SparkUtils.setMaster(conf);
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        /**
         * 查看配置文件是否是本地测试，若是本地测试那么创建一个SQLContext   如果是集群测试HiveContext
         */
        SQLContext sqlContext = SparkUtils.getSQLContext(sparkContext);

        /**
         * 基于本地测试生成模拟测试数据，如果在集群中运行的话，直接操作Hive中的表就可以
         * 本地模拟数据注册成一张临时表
         * monitor_flow_action	数据表：监控车流量所有数据
         * monitor_camera_info	标准表：卡扣对应摄像头标准表
         */
        if(ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)){
            //本地
            MockData.mock(sparkContext,sqlContext);
        }else{
            //集群
            sqlContext.sql("use traffic");
        }

/**------------------------------------------------------------------------------------*/
/**------------------------------------------------------------------------------------*/
        /**
         * 给定一个时间段，统计出卡口数量的正常数量，异常数量，还有通道数
         * 异常数：每一个卡口都会有n个摄像头对应每一个车道，如果这一段时间内卡口的信息没有第N车道的信息的话就说明这个卡口存在异常。
         * 这需要拿到一份数据（每一个卡口对应的摄像头的编号）,模拟数据在monitor_camera_info临时表中
         *
         * 从配置文件my.properties中拿到spark.local.taskId.monitorFlow的taskId
         */
        long taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_MONITOR);
        if(taskId == 0L){
            System.out.println("args is null.....");
            return;
        }

        /**
         * 获取ITaskDAO的对象，通过taskId查询出来的数据封装到Task（自定义）对象
         */
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        Task task = taskDAO.findTaskById(taskId);
        if (task == null) return;

        /**
         * task.getTaskParams()是一个string格式的字符串  封装到taskParamsJsonObject
         * 将 task_parm字符串转换成json格式数据。
         */
        JSONObject jsonObject = JSONObject.parseObject(task.getTaskParams());

        /**
         * 通过params（json字符串）查询monitor_flow_action
         *
         * 获取指定日期内检测的monitor_flow_action中车流量数据，返回JavaRDD<Row>
         */
        JavaRDD<Row> cameraRDD = SparkUtils.getCameraRDDByDateRange(sqlContext, jsonObject);

        /**
         * 持久化
         */
        cameraRDD = cameraRDD.cache();

        /**
         * 自定义累加器，维护多个值
         */
        Accumulator<String> accumulator = sparkContext.accumulator("", new MonitorAndCameraStateAccumulator());

        /**
         * 将row类型的RDD 转换成kv格式的RDD   k:monitor_id  v:row
         */
        JavaPairRDD<String, Row> monitor2DetailRDD = getMonitor2DetailRDD(cameraRDD);

        /**
         * monitor2DetailRDD进行持久化
         */
        monitor2DetailRDD = monitor2DetailRDD.cache();

        /**
         * 按照卡扣号分组，对应的数据是：每个卡扣号(monitor)对应的Row信息
         * 由于一共有9个卡扣号，这里groupByKey后一共有9组数据。
         */
        JavaPairRDD<String, Iterable<Row>> stringIterableJavaPairRDD = monitor2DetailRDD.groupByKey();
        //持久化
        stringIterableJavaPairRDD = stringIterableJavaPairRDD.cache();

        /**
         * 遍历分组后的RDD，拼接字符串
         * 数据中一共就有9个monitorId信息，那么聚合之后的信息也是9条
         * monitor_id=|cameraIds=|area_id=|camera_count=|carCount=
         * 例如:("0005","monitorId=0005|areaId=02|camearIds=09200,03243,02435,03232|cameraCount=4|carCount=100")
         */
        JavaPairRDD<String, String> aggregateMonitorId2DetailRDD = aggreagteByMonitor(stringIterableJavaPairRDD);

        /**
         * 检测卡扣状态
         * carCount2MonitorRDD
         * K:car_count V:monitor_id
         * RDD(卡扣对应车流量总数,对应的卡扣号)
         * 自定义累加器计算路口摄像头各种情况
         */
        JavaPairRDD<Integer, String> carCount2MonitorRDD =  checkMonitorState(sparkContext,sqlContext,aggregateMonitorId2DetailRDD,taskId,jsonObject,accumulator);
        //持久化
        carCount2MonitorRDD = carCount2MonitorRDD.cache();

        //action 类算子触发以上操作,接下来需要读取累加器数据
        carCount2MonitorRDD.count();

        /**
         * 往数据库表  monitor_state 中保存 累加器累加的五个状态
         */
        saveMonitorState(taskId,accumulator);

/**------------------------------------------------------------------------------------*/
/**------------------------------------------------------------------------------------*/

        /**
         * 获取车流排名前N的卡扣号
         * 并放入数据库表  topn_monitor_car_count 中
         * return  KV格式的RDD  K：monitor_id V:monitor_id
         * 返回的是topN的(monitor_id,monitor_id)
         */
        JavaPairRDD<String, String> topNMonitor2CarFlow = getTopNMonitorCarFlow(sparkContext,taskId,jsonObject,carCount2MonitorRDD);

        /**
         * 获取topN卡口的车流量具体信息，存入数据库表 topn_monitor_detail_info 中
         */
        getTopNDetails(taskId,topNMonitor2CarFlow,monitor2DetailRDD);

/**------------------------------------------------------------------------------------*/
/**------------------------------------------------------------------------------------*/

        /**
         * 获取高速通过的TOPN卡扣
         * return List<monitorId>
         */
        List<String> top5MonitorIds = speedTopNMonitor(stringIterableJavaPairRDD);
        for (String monitorId : top5MonitorIds) {
            System.out.println("车辆经常高速通过的卡扣	monitorId:"+monitorId);
        }

/**------------------------------------------------------------------------------------*/
/**------------------------------------------------------------------------------------*/

        /**
         * 获取车辆高速通过的TOPN卡扣，每一个卡扣中车辆速度最快的前10名，并存入数据库表 top10_speed_detail 中
         */
        getMonitorDetails(sparkContext,taskId,top5MonitorIds,monitor2DetailRDD);

/**------------------------------------------------------------------------------------*/
/**------------------------------------------------------------------------------------*/

        /**
         * 卡扣碰撞分析，直接打印结果（两辆车车牌号一样称为碰撞）
         */
        areaCarPeng(sqlContext,jsonObject);

/**------------------------------------------------------------------------------------*/
/**------------------------------------------------------------------------------------*/

        /**
         * 程序运行结束
         */
        System.out.println("******All is finished*******");
        sparkContext.close();
    }

    /**
     * 将RDD转换成K,V格式的RDD
     * @param cameraRDD
     * @return
     */
    private static JavaPairRDD<String, Row> getMonitor2DetailRDD(JavaRDD<Row> cameraRDD) {

        JavaPairRDD<String, Row> stringRowJavaPairRDD = cameraRDD.mapToPair(new PairFunction<Row, String, Row>() {

            private static final long serialVersionUID = 7248682256027097356L;

            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                /**
                 * row.getString(1) 是得到monitor_id 。
                 */
                return new Tuple2<String, Row>(row.getString(1), row);
            }
        });
        return stringRowJavaPairRDD;
    }

    /**
     * 按照monitor_id进行聚合
     * @param monitorId2Detail
     * @return ("monitorId","monitorId=xxx|areaId=xxx|cameraIds=xxx|cameraCount=xxx|carCount=xxx")
     * ("0005","monitorId=0005|areaId=02|camearIds=09200,03243,02435,03232|cameraCount=4|carCount=100")
     * 假设其中一条数据是以上这条数据，那么说明在这个0005卡扣下有4个camera,那么这个卡扣一共通过了100辆车信息.
     */
    private static JavaPairRDD<String,String> aggreagteByMonitor(JavaPairRDD<String,Iterable<Row>> monitorId2Detail)
    {
        /**
         * <monitor_id,List<Row> 集合里面的一个row记录代表的是camera的信息，row也可以说是代表的一辆车的信息。>
         * 一个monitor_id对应一条记录
         * 为什么使用mapToPair来遍历数据，因为我们要操作的返回值是每一个monitorid 所对应的详细信息
         */
        JavaPairRDD<String, String> stringStringJavaPairRDD = monitorId2Detail.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, String, String>() {

            private static final long serialVersionUID = 2655327207840146172L;

            @Override
            public Tuple2<String, String> call(Tuple2<String, Iterable<Row>> iterableTuple2) throws Exception {
                String monitorId = iterableTuple2._1;
                Iterator<Row> rowIterator = iterableTuple2._2.iterator();
                //同一个monitorId下，对应的所有的不同的cameraId,list.count方便知道此monitor下对应多少个cameraId
                ArrayList<String> arrayList = new ArrayList<>();
                //同一个monitorId下，对应的所有的不同的camearId信息
                StringBuilder builder = new StringBuilder();

                int count = 0;
                String areaId = "";//区域ID
                /**
                 * 这个while循环  代表的是当前的这个卡扣一共经过了多少辆车，   一辆车的信息就是一个row
                 */
                while (rowIterator.hasNext()) {
                    Row row = rowIterator.next();
                    areaId = row.getString(7);
                    String cameraId = row.getString(2);
                    if (!arrayList.contains(cameraId)) arrayList.add(cameraId);
                    //针对同一个卡扣 monitor，append所有所属得cameraId
                    if (!builder.toString().contains(cameraId)) {
                        builder.append("," + cameraId);
                    }
                    //这里的count就代表的车辆数，一个row一辆车
                    count++;
                }
                int cameraCount = arrayList.size();

                //monitorId=0001|areaId=03|cameraIds=00001,00002,00003|cameraCount=3|carCount=100
                String infos = Constants.FIELD_MONITOR_ID + "=" + monitorId + "|"
                        + Constants.FIELD_AREA_ID + "=" + areaId + "|"
                        + Constants.FIELD_CAMERA_IDS + "=" + builder.toString().substring(1) + "|"
                        + Constants.FIELD_CAMERA_COUNT + "=" + cameraCount + "|"
                        + Constants.FIELD_CAR_COUNT + "=" + count;
                return new Tuple2<String, String>(monitorId, infos);
            }
        });
        return stringStringJavaPairRDD;
    }

    /**
     * 检测卡口状态
     * 自定义累加器计算路口摄像头各种情况
     * @return RDD(实际卡扣对应车流量总数,对应的卡扣号)
     */
    private static JavaPairRDD<Integer,String> checkMonitorState(
            JavaSparkContext sc,
            SQLContext sqlContext,
            JavaPairRDD<String,String> monitorId2CameraCountRDD,
            final long taskId,
            JSONObject jsonObject,
            final Accumulator<String> accumulator)
    {
        /**
         * 从monitor_camera_info标准表中查询出来每一个卡口对应的camera的数量
         */
        String sqlText = "SELECT * FROM monitor_camera_info";
        DataFrame standardDF = sqlContext.sql(sqlText);
        JavaRDD<Row> standardRDD = standardDF.javaRDD();

        /**
         * 使用mapToPair算子将standardRDD变成KV格式的RDD
         * monitorId2CameraId   :
         * (K:monitor_id  v:camera_id)
         */
        JavaPairRDD<String, String> stringStringJavaPairRDD = standardRDD.mapToPair(new PairFunction<Row, String, String>() {
            private static final long serialVersionUID = -6456263129364206762L;

            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                return new Tuple2<String, String>(row.getString(0), row.getString(1));
            }
        });

        /**
         * 对每一个卡扣下面的信息进行统计，统计出来camera_count（这个卡扣下一共有多少个摄像头）,camera_ids(这个卡扣下，所有的摄像头编号拼接成的字符串)
         * 返回：("monitorId","cameraIds=xxx|cameraCount=xxx")
         * 例如：("0008","cameraIds=02322,01213,03442|cameraCount=3")
         * 如何来统计？
         * 	1、按照monitor_id分组
         * 	2、使用mapToPair遍历，遍历的过程可以统计
         */
        JavaPairRDD<String, String> stringStringJavaPairRDD1 = stringStringJavaPairRDD.groupByKey().mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, String, String>() {
            private static final long serialVersionUID = 6677135588205971974L;

            @Override
            public Tuple2<String, String> call(Tuple2<String, Iterable<String>> stringIterableTuple2) throws Exception {
                String monitorId = stringIterableTuple2._1;
                Iterator<String> iterator = stringIterableTuple2._2.iterator();
                int count = 0;
                StringBuilder stringBuilder = new StringBuilder();
                while (iterator.hasNext()) {
                    String next = iterator.next();
                    stringBuilder.append("," + next);
                    count++;
                }
                //cameraIds=00001,00002,00003,00004|cameraCount=4
                String cameraInfos = Constants.FIELD_CAMERA_IDS + "=" + stringBuilder.toString().substring(1) + "|"
                        + Constants.FIELD_CAMERA_COUNT + "=" + count;

                return new Tuple2<String, String>(monitorId, cameraInfos);
            }
        });

        /**
         * 将两个RDD进行比较，join  leftOuterJoin
         * 为什么使用左外连接？ 左：标准表里面的信息  右：实际信息
         */
        JavaPairRDD<String, Tuple2<String, Optional<String>>> joinResultRDD = stringStringJavaPairRDD1.leftOuterJoin(monitorId2CameraCountRDD);

        /**
         * return carCount2MonitorId 最终返回的K,V格式的数据
         * K：实际监测数据中某个卡扣对应的总车流量
         * V：实际监测数据中这个卡扣 monitorId
         */
        JavaPairRDD<Integer, String> integerStringJavaPairRDD = joinResultRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<String, Tuple2<String, Optional<String>>>>, Integer, String>() {
            private static final long serialVersionUID = 4282674968079621536L;

            @Override
            public Iterable<Tuple2<Integer, String>> call(Iterator<Tuple2<String, Tuple2<String, Optional<String>>>> tuple2Iterator) throws Exception {
                ArrayList<Tuple2<Integer, String>> arrayList = new ArrayList<>();

                while (tuple2Iterator.hasNext()) {
                    Tuple2<String, Tuple2<String, Optional<String>>> stringTuple2Tuple2 = tuple2Iterator.next();
                    String monitorId = stringTuple2Tuple2._1;
                    String standardCameraInfos = stringTuple2Tuple2._2._1;
                    Optional<String> factCameraInfosOptional = stringTuple2Tuple2._2._2;
                    String factCameraInfos = "";
                    //	boolean isPresent()
                    //如果值存在则方法会返回true，否则返回 false
                    if (factCameraInfosOptional.isPresent()) {
                        //这里面是实际检测数据中有标准卡扣信息
                        factCameraInfos = factCameraInfosOptional.get();
                    } else {
                        //这里面是实际检测数据中没有标准卡扣信息(摄像头损坏，无法拍摄故没有行车数据)
                        String standardCameraIds = StringUtils.getFieldFromConcatString(standardCameraInfos, "\\|", Constants.FIELD_CAMERA_IDS);
                        String[] split = standardCameraIds.split(",");
                        int abnoramlCameraCount = split.length;//实际应有得摄像头数量

                        StringBuilder stringBuilder = new StringBuilder();
                        for (String caneraId : split) {
                            stringBuilder.append("," + caneraId);//拼接所有摄像头id
                        }

                        //abnormalMonitorCount=1|abnormalCameraCount=3|abnormalMonitorCameraInfos="0002":07553,07554,07556
                        accumulator.add(
                                Constants.FIELD_ABNORMAL_MONITOR_COUNT + "=1|"
                                        + Constants.FIELD_ABNORMAL_CAMERA_COUNT + "=" + abnoramlCameraCount + "|"
                                        + Constants.FIELD_ABNORMAL_MONITOR_CAMERA_INFOS + "=" + monitorId + ":" + stringBuilder.toString().substring(1));
                        //跳出了本次while
                        continue;
                    }

                    /**
                     * 从实际数据拼接的字符串中获取摄像头数
                     */
                    int factCameraCount = Integer.parseInt(StringUtils.getFieldFromConcatString(factCameraInfos, "\\|", Constants.FIELD_CAMERA_COUNT));
                    /**
                     * 从标准数据拼接的字符串中获取摄像头数
                     */
                    int standardCameraCount = Integer.parseInt(StringUtils.getFieldFromConcatString(standardCameraInfos, "\\|", Constants.FIELD_CAMERA_COUNT));

                    if (factCameraCount == standardCameraCount) {
                        /*
                         * 	1、正常卡口数量
                         * 	2、异常卡口数量
                         * 	3、正常通道（此通道的摄像头运行正常）数，通道就是摄像头
                         * 	4、异常卡口数量中哪些摄像头异常，需要保存摄像头的编号
                         */
                        accumulator.add(Constants.FIELD_NORMAL_MONITOR_COUNT + "=1|" + Constants.FIELD_NORMAL_CAMERA_COUNT + "=" + factCameraCount);//正常卡口数量
                    } else {
                        /**
                         * 从实际数据拼接的字符串中获取摄像编号集合
                         */
                        String factCameraIds = StringUtils.getFieldFromConcatString(factCameraInfos, "\\|", Constants.FIELD_CAMERA_IDS);

                        /**
                         * 从标准数据拼接的字符串中获取摄像头编号集合
                         */
                        String standardCameraIds = StringUtils.getFieldFromConcatString(standardCameraInfos, "\\|", Constants.FIELD_CAMERA_IDS);

                        List<String> factCameraIdList = Arrays.asList(factCameraIds.split(","));
                        List<String> standardCameraIdList = Arrays.asList(standardCameraIds.split(","));
                        StringBuilder abnormalCameraInfos = new StringBuilder();
//						System.out.println("factCameraIdList:"+factCameraIdList);
//						System.out.println("standardCameraIdList:"+standardCameraIdList);
                        int abnormalCameraCount = 0;//不正常摄像头数
                        int normalCameraCount = 0;//正常摄像头数
                        for (String cameraId : standardCameraIdList) {
                            if (!factCameraIdList.contains(cameraId)) {
                                abnormalCameraCount++;
                                abnormalCameraInfos.append("," + cameraId);
                            }
                        }
                        normalCameraCount = standardCameraIdList.size() - abnormalCameraCount;
                        //往累加器中更新状态
                        accumulator.add(
                                Constants.FIELD_NORMAL_CAMERA_COUNT + "=" + normalCameraCount + "|"
                                        + Constants.FIELD_ABNORMAL_MONITOR_COUNT + "=1|"
                                        + Constants.FIELD_ABNORMAL_CAMERA_COUNT + "=" + abnormalCameraCount + "|"
                                        + Constants.FIELD_ABNORMAL_MONITOR_CAMERA_INFOS + "=" + monitorId + ":" + abnormalCameraInfos.toString().substring(1));
                    }
                    //从实际数据拼接到字符串中获取车流量
                    int carCount = Integer.parseInt(StringUtils.getFieldFromConcatString(factCameraInfos, "\\|", Constants.FIELD_CAR_COUNT));
                    arrayList.add(new Tuple2<Integer, String>(carCount, monitorId));
                }
                //最后返回的list是实际监测到的数据中，list[(卡扣对应车流量总数,对应的卡扣号),... ...]
                return arrayList;
            }
        });
        return integerStringJavaPairRDD;
    }

    /**
     * 往数据库中保存 累加器累加的五个状态
     *
     * @param taskId
     * @param monitorAndCameraStateAccumulator
     */
    private static void saveMonitorState(
            Long taskId,
            Accumulator<String> monitorAndCameraStateAccumulator)
    {

        /**
         * 累加器中值能在Executor段读取吗？
         * 		不能
         * 这里的读取时在Driver中进行的
         */
        String accumulatorVal = monitorAndCameraStateAccumulator.value();
        String normalMonitorCount = StringUtils.getFieldFromConcatString(accumulatorVal, "\\|", Constants.FIELD_NORMAL_MONITOR_COUNT);
        String normalCameraCount = StringUtils.getFieldFromConcatString(accumulatorVal, "\\|", Constants.FIELD_NORMAL_CAMERA_COUNT);
        String abnormalMonitorCount = StringUtils.getFieldFromConcatString(accumulatorVal, "\\|", Constants.FIELD_ABNORMAL_MONITOR_COUNT);
        String abnormalCameraCount = StringUtils.getFieldFromConcatString(accumulatorVal, "\\|", Constants.FIELD_ABNORMAL_CAMERA_COUNT);
        String abnormalMonitorCameraInfos = StringUtils.getFieldFromConcatString(accumulatorVal, "\\|", Constants.FIELD_ABNORMAL_MONITOR_CAMERA_INFOS);

        /**
         * 这里面只有一条记录
         */
        MonitorState monitorState = new MonitorState(taskId, normalMonitorCount, normalCameraCount, abnormalMonitorCount, abnormalCameraCount, abnormalMonitorCameraInfos);

       /**
        * 向数据库表monitor_state中添加累加器累计的各个值
        */
        IMonitorDAO monitorDAO = DAOFactory.getMonitorDAO();
        monitorDAO.insertMonitorState(monitorState);
    }

    /**
     * 获取卡口流量的前N名，并且持久化到数据库中
     * N是在数据库条件中取值
     * @param taskId
     * @param jsonObject
     * @param carCount2MonitorRDD ----RDD(卡扣对应的车流量总数,对应的卡扣号)
     * 有什么作用？ 当某一个卡口的流量这几天突然暴增和往常的流量不相符，交管部门应该找一下原因，是什么问题导致的，应该到现场去疏导车辆。
     */
    private static JavaPairRDD<String,String> getTopNMonitorCarFlow(
            JavaSparkContext sparkContext,
            long taskId,
            JSONObject jsonObject,
            JavaPairRDD<Integer,String> carCount2MonitorRDD)
    {
        //要计算取出卡口流量排名前topNumFromParams得卡口号
        int topNumFromParams = Integer.parseInt(ParamUtils.getParam(jsonObject,Constants.FIELD_TOP_NUM));
        //sortByKey(false) 数字从大到小排列
        List<Tuple2<Integer, String>> topNCarCount = carCount2MonitorRDD.sortByKey(false).take(topNumFromParams);

        //封装到对象中
        ArrayList<TopNMonitor2CarCount> topNMonitor2CarCounts = new ArrayList<>();
        for (Tuple2<Integer,String> tuple : topNCarCount)
        {
            TopNMonitor2CarCount topNMonitor2CarCount = new TopNMonitor2CarCount(taskId,tuple._2,tuple._1);
            topNMonitor2CarCounts.add(topNMonitor2CarCount);
        }

        /**
         * 得到DAO 将数据插入数据库
         * 向数据库表 topn_monitor_car_count 中插入车流量最多的TopN数据
         */
        IMonitorDAO ITopNMonitor2CarCountDAO = DAOFactory.getMonitorDAO();
        ITopNMonitor2CarCountDAO.insertBatchTopN(topNMonitor2CarCounts);

        /**
         * monitorId2MonitorIdRDD ---- K:monitor_id V:monitor_id
         * 获取topN卡口的详细信息
         * monitorId2MonitorIdRDD.join(monitorId2RowRDD)
         */
        List<Tuple2<String, String>> monitorId2CarCounts = new ArrayList<>();
        for(Tuple2<Integer,String> t : topNCarCount){
            monitorId2CarCounts.add(new Tuple2<String, String>(t._2, t._2));
        }
        JavaPairRDD<String, String> monitorId2MonitorIdRDD = sparkContext.parallelizePairs(monitorId2CarCounts);
        return monitorId2MonitorIdRDD;
    }

    /**
     * 获取topN 卡口的车流量具体信息，存入数据库表 topn_monitor_detail_info 中
     * @param taskId
     * @param topNMonitor2CarFlow ---- (monitorId,monitorId)
     * @param monitor2DetailRDD ---- (monitorId,Row)
     */
    private static void getTopNDetails(
            final long taskId,
            JavaPairRDD<String, String> topNMonitor2CarFlow,
            JavaPairRDD<String, Row> monitor2DetailRDD)
    {
        /**
         * 优化点：
         * 因为topNMonitor2CarFlow 里面有只有5条数据，可以将这五条数据封装到广播变量中，然后遍历monitor2DetailRDD ，每遍历一条数据与广播变量中的值作比对。
         */
        topNMonitor2CarFlow.join(monitor2DetailRDD).mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Row>>, String, Row>() {
            private static final long serialVersionUID = -8628992783270586303L;
            @Override
            public Tuple2<String, Row> call(Tuple2<String, Tuple2<String, Row>> stringTuple2Tuple2) throws Exception {
                return new Tuple2<String, Row>(stringTuple2Tuple2._1, stringTuple2Tuple2._2._2);
            }
        }).foreachPartition(new VoidFunction<Iterator<Tuple2<String, Row>>>() {
            private static final long serialVersionUID = 2297901276427150610L;
            @Override
            public void call(Iterator<Tuple2<String, Row>> tuple2Iterator) throws Exception {
                ArrayList<TopNMonitorDetailInfo> arrayList = new ArrayList<>();
                while (tuple2Iterator.hasNext())
                {
                    Tuple2<String, Row> tuple2 = tuple2Iterator.next();
                    Row row = tuple2._2;
                    TopNMonitorDetailInfo m = new TopNMonitorDetailInfo(taskId, row.getString(0), row.getString(1), row.getString(2), row.getString(3), row.getString(4), row.getString(5), row.getString(6));
                    arrayList.add(m);
                }
                /**
                 * 将topN的卡扣车流量明细数据 存入topn_monitor_detail_info 表中
                 */
                IMonitorDAO monitorDAO = DAOFactory.getMonitorDAO();
                monitorDAO.insertBatchMonitorDetails(arrayList);
            }
        });

        /********************************使用广播变量来实现************************************/
//		JavaSparkContext jsc = new JavaSparkContext(topNMonitor2CarFlow.context());
//		//将topNMonitor2CarFlow（只有5条数据）转成非K,V格式的数据，便于广播出去
//		JavaRDD<String> topNMonitorCarFlow = topNMonitor2CarFlow.map(new Function<Tuple2<String,String>, String>() {
//			private static final long serialVersionUID = 1L;
//			@Override
//			public String call(Tuple2<String, String> tuple) throws Exception {
//				return tuple._1;
//			}
//		});
//		List<String> topNMonitorIds = topNMonitorCarFlow.collect();
//		final Broadcast<List<String>> broadcast_topNMonitorIds = jsc.broadcast(topNMonitorIds);
//		JavaPairRDD<String, Row> filterTopNMonitor2CarFlow = monitor2DetailRDD.filter(new Function<Tuple2<String,Row>, Boolean>() {
//			private static final long serialVersionUID = 1L;
//			@Override
//			public Boolean call(Tuple2<String, Row> monitorTuple) throws Exception {
//				return broadcast_topNMonitorIds.value().contains(monitorTuple._1);
//			}
//		});
//		filterTopNMonitor2CarFlow.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Row>>>() {
//			private static final long serialVersionUID = 1L;
//			@Override
//			public void call(Iterator<Tuple2<String, Row>> t) throws Exception {
//				List<TopNMonitorDetailInfo> monitorDetailInfos = new ArrayList<>();
//				while(t.hasNext()){
//					Tuple2<String, Row> tuple = t.next();
//					Row row = tuple._2;
//					TopNMonitorDetailInfo m = new TopNMonitorDetailInfo(taskId, row.getString(0), row.getString(1), row.getString(2), row.getString(3), row.getString(4), row.getString(5), row.getString(6));
//					monitorDetailInfos.add(m);
//				}
//				/**
//				 * 将topN的卡扣车流量明细数据 存入topn_monitor_detail_info 表中
//				 */
//				IMonitorDAO monitorDAO = DAOFactory.getMonitorDAO();
//				monitorDAO.insertBatchMonitorDetails(monitorDetailInfos);
//			}
//		});

    }

    /**
     * 获取经常高速通过的TOPN卡扣 , 返回车辆经常高速通过的卡扣List
     *
     * 1、每一辆车都有speed    按照速度划分是否是高速 中速 普通 低速
     * 2、每一辆车的车速都在一个车速段     对每一个卡扣进行聚合   拿到高速通过 中速通过  普通  低速通过的车辆各是多少辆
     * 3、四次排序   先按照高速通过车辆数   中速通过车辆数   普通通过车辆数   低速通过车辆数
     * @param groupByMonitorId ---- (monitorId ,Iterable[Row])
     * @return List<MonitorId> 返回车辆经常高速通过的卡扣List
     */
    private static List<String> speedTopNMonitor(JavaPairRDD<String, Iterable<Row>> groupByMonitorId) {

        /**
         * key:自定义的类  value：卡扣ID
         */
        JavaPairRDD<SpeedSortKey, String> speedSortKey2MonitorId = groupByMonitorId.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, SpeedSortKey, String>() {
            private static final long serialVersionUID = 682530690712634353L;

            @Override
            public Tuple2<SpeedSortKey, String> call(Tuple2<String, Iterable<Row>> tuple2) throws Exception {
                String monitorId = tuple2._1;
                Iterator<Row> rowIterator = tuple2._2.iterator();

                /**
                 * 遍历统计每个卡扣下 高速 中速 正常 以及低速通过的车辆数
                 */
                long lowSpeed = 0;
                long normalSpeed = 0;
                long mediumSpeed = 0;
                long highSpeed = 0;

                while (rowIterator.hasNext()) {
                    int speed = StringUtils.convertStringtoInt(rowIterator.next().getString(5));
                    if (speed >= 0 && speed < 60) {
                        lowSpeed++;
                    } else if (speed >= 60 && speed < 90) {
                        normalSpeed++;
                    } else if (speed >= 90 && speed < 120) {
                        mediumSpeed++;
                    } else if (speed >= 120) {
                        highSpeed++;
                    }
                }
                SpeedSortKey speedSortKey = new SpeedSortKey(lowSpeed, normalSpeed, mediumSpeed, highSpeed);
                return new Tuple2<SpeedSortKey, String>(speedSortKey, monitorId);
            }
        });

        /**
         * key:自定义的类  value：卡扣ID
         */
        JavaPairRDD<SpeedSortKey, String> sortBySpeedCount = speedSortKey2MonitorId.sortByKey(false);

        /**
         * 取出前5个经常速度高的卡扣
         */
        List<Tuple2<SpeedSortKey, String>> take = sortBySpeedCount.take(5);

        List<String> monitorIds = new ArrayList<>();
        for (Tuple2<SpeedSortKey, String> tuple : take) {
            monitorIds.add(tuple._2);
            System.out.println("monitor_id = "+tuple._2+"-----"+tuple._1);
        }
        return monitorIds;
    }

    /**
     * 获取车辆经常高速通过的TOPN卡扣，每一个卡扣中车辆速度最快的前10名，并存入数据库表 top10_speed_detail 中
     * @param sc
     * @param taskId
     * @param top5MonitorIds
     * @param monitor2DetailRDD
     */
    private static void getMonitorDetails(JavaSparkContext sc , final long taskId,List<String> top5MonitorIds, JavaPairRDD<String, Row> monitor2DetailRDD) {

        /**
         * 广播变量,top5MonitorIds这个集合里面只有monitor_id
         */
        final Broadcast<List<String>> top5MonitorIdsBroadcast = sc.broadcast(top5MonitorIds);

        /**
         * 从monitor2DetailRDD中过滤出来包含在top5MonitorIds集合的卡扣的详细信息
         */
        monitor2DetailRDD.filter(new Function<Tuple2<String, Row>, Boolean>() {
            private static final long serialVersionUID = -7643612493677047347L;

            @Override
            public Boolean call(Tuple2<String, Row> tuple2) throws Exception {
                String monitorId = tuple2._1;
                List<String> list = top5MonitorIdsBroadcast.value();
                return list.contains(monitorId);
            }
        })
                .groupByKey()
                .foreach(new VoidFunction<Tuple2<String, Iterable<Row>>>() {
                    private static final long serialVersionUID = 1588255469805727415L;

                    @Override
                    public void call(Tuple2<String, Iterable<Row>> tuple2) throws Exception {
                        Iterator<Row> rowsIterator = tuple2._2.iterator();

                        Row[] top10Cars = new Row[10];
                        while (rowsIterator.hasNext()) {
                            Row row = rowsIterator.next();
                            long speed = Long.valueOf(row.getString(5));
                            for (int i = 0; i < top10Cars.length; i++) {
                                if (top10Cars[i] == null) {
                                    top10Cars[i] = row;
                                    break;
                                } else {
                                    long _speed = Long.valueOf(top10Cars[i].getString(5));
                                    if (speed > _speed) {
                                        for (int j = 9; j > i; j--) {
                                            top10Cars[j] = top10Cars[j - 1];
                                        }
                                        top10Cars[i] = row;
                                        break;
                                    }
                                }
                            }
                        }
                        /**
                         * 将车辆通过速度最快的前N个卡扣中每个卡扣通过的车辆的速度最快的前10名存入数据库表 top10_speed_detail中
                         */
                        IMonitorDAO monitorDAO = DAOFactory.getMonitorDAO();
                        List<TopNMonitorDetailInfo> topNMonitorDetailInfos = new ArrayList<>();
                        for (Row row : top10Cars) {
                            topNMonitorDetailInfos.add(new TopNMonitorDetailInfo(taskId, row.getString(0), row.getString(1), row.getString(2), row.getString(3), row.getString(4), row.getString(5), row.getString(6)));
                        }
                        monitorDAO.insertBatchTop10Details(topNMonitorDetailInfos);
                    }
                });
            }

    /**
     * 卡扣碰撞分析（两车同一车牌）
     * 假设数据如下：
     * area1卡扣:["0000", "0001", "0002", "0003"]
     * area2卡扣:["0004", "0005", "0006", "0007"]
     */
    private static void areaCarPeng(
            SQLContext sqlContext,
            JSONObject taskParamsJsonObject)
    {
        List<String> monitorIds1 = Arrays.asList("0000", "0001", "0002", "0003");
        List<String> monitorIds2 = Arrays.asList("0004", "0005", "0006", "0007");

        // 通过两堆卡扣号，分别取数据库（本地模拟的两张表）中查询数据
        JavaRDD<Row> areaRDD1 = getAreaRDDByMonitorIds(sqlContext, taskParamsJsonObject, monitorIds1);
        JavaRDD<Row> areaRDD2 = getAreaRDDByMonitorIds(sqlContext, taskParamsJsonObject, monitorIds2);

        JavaRDD<String> area1Cars = areaRDD1.map(new Function<Row, String>() {
            private static final long serialVersionUID = 1916131024319359739L;
            @Override
            public String call(Row row) throws Exception {
                return row.getAs("car")+"";//获取车牌号
            }
        }).distinct();//去重
        JavaRDD<String> area2Cars = areaRDD2.map(new Function<Row, String>() {
            private static final long serialVersionUID = -1758592973693712778L;
            @Override
            public String call(Row row) throws Exception {
                return row.getAs("car")+"";//获取车牌号
            }
        }).distinct();//去重
        //取交集，交集结果会去重
        JavaRDD<String> intersection = area1Cars.intersection(area2Cars);

        intersection.foreach(new VoidFunction<String>() {
            private static final long serialVersionUID = -7729125540058703285L;
            @Override
            public void call(String car) throws Exception {
                System.out.println("area1 与 area2 共同车辆="+car);
            }
        });

    }

    private static JavaRDD<Row> getAreaRDDByMonitorIds(SQLContext sqlContext,JSONObject taskParamsJsonObject, List<String> monitorId1) {
        String startTime = ParamUtils.getParam(taskParamsJsonObject, Constants.PARAM_START_DATE);
        String endTime = ParamUtils.getParam(taskParamsJsonObject, Constants.PARAM_END_DATE);
        String sql = "SELECT * "
                + "FROM monitor_flow_action"
                + " WHERE date >='" + startTime + "' "
                + " AND date <= '" + endTime+ "' "
                + " AND monitor_id in (";

        for(int i = 0 ; i < monitorId1.size() ; i++){
            sql += "'"+monitorId1.get(i) + "'";

            if( i  < monitorId1.size() - 1 ){
                sql += ",";
            }
        }

        sql += ")";
        return sqlContext.sql(sql).javaRDD();
    }

    }
