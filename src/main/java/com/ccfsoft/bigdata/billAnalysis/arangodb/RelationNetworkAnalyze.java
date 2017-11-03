package com.ccfsoft.bigdata.billAnalysis.arangodb;

import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBException;
import com.arangodb.ArangoDatabase;
import com.arangodb.ArangoGraph;
import com.arangodb.entity.EdgeDefinition;
import com.arangodb.entity.EdgeEntity;
import com.arangodb.entity.GraphEntity;
import com.arangodb.entity.VertexEntity;

import com.ccfsoft.bigdata.billAnalysis.arangodb.entity.Node;
import com.ccfsoft.bigdata.billAnalysis.arangodb.entity.NodeEdge;
import com.ccfsoft.bigdata.utils.PropertyConstants;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Collection;

/**
 * 关系网络构建和入库
 */
public class RelationNetworkAnalyze {
    protected static ArangoDB arangoDB;
    protected static ArangoDatabase db;

    protected static final String DB = PropertyConstants.getPropertiesKey("arangoDB");//数据库名称
    protected static final String GRAPH_NAME = "BillGraph";//图形名称
    protected static final String VERTEXT_COLLECTION_NAME = "nodes";//顶点集合
    protected static final String EDGE_COLLECTION_NAME = "edges";//边集合

    public static void relationNetworkAnalyze(SparkSession spark) {

        // 加载Arangodb配置
        if (arangoDB == null) {
            arangoDB = new ArangoDB.Builder()
                    .host(PropertyConstants.getPropertiesKey("arangoDB_ip"),
                            Integer.valueOf(PropertyConstants.getPropertiesKey("arangoDB_port")))
                    .user(PropertyConstants.getPropertiesKey("arangoDB_user"))
                    .password(PropertyConstants.getPropertiesKey("arangoDB_passwd"))
                    .build();
        }

        if(!arangoDB.getDatabases().contains(DB))
        {
            arangoDB.createDatabase(DB);
        }

        RelationNetworkAnalyze.db = arangoDB.db(DB);

        boolean isGraphExist = false;
        for(GraphEntity graphEntity: db.getGraphs()){
            if(GRAPH_NAME.equals(graphEntity.getName()))
                isGraphExist = true;
        }

        // 构图
        if(!isGraphExist)
        {
            db.createCollection(VERTEXT_COLLECTION_NAME);
            final Collection<EdgeDefinition> edgeDefinitions = new ArrayList<EdgeDefinition>();
            final EdgeDefinition edgeDefinition = new EdgeDefinition().collection(EDGE_COLLECTION_NAME)
                    .from(VERTEXT_COLLECTION_NAME).to(VERTEXT_COLLECTION_NAME);
            edgeDefinitions.add(edgeDefinition);
            db.createGraph(GRAPH_NAME, edgeDefinitions, null);
        }


        // 绘制顶点
        Dataset<Row> nodesDf = spark.sql("select distinct(*) from (select T1.own_phone from BILL T1 union all select T2.other_phone from BILL T2) T");

        for (Row row : nodesDf.collectAsList()) {
            String strRow = row.getString(0);
            // 重复节点不绘制
            if (!db.collection(VERTEXT_COLLECTION_NAME).documentExists(strRow))
                createVertex(new Node(strRow, Long.valueOf(strRow)));
        }

        // 绘制所有的边
        Dataset<Row> relationDf = spark.sql("select T.own_phone,T.other_phone,T.begin_date,T.begin_time,T.talk_time from BILL T");
        for (Row row : relationDf.collectAsList()) {
//            Long own_phone = row.getLong(0);
//            Long other_phone = row.getLong(1);
            String own_phone = row.getString(0);
            String other_phone = row.getString(1);

//            saveEdge(new NodeEdge(VERTEXT_COLLECTION_NAME + "/" + own_phone.toString(),
//                    VERTEXT_COLLECTION_NAME + "/" + other_phone.toString(),
//                    row.getString(2) + " " + row.getString(3), Integer.valueOf(row.getString(4))));

            saveEdge(new NodeEdge(VERTEXT_COLLECTION_NAME + "/" + own_phone,
                    VERTEXT_COLLECTION_NAME + "/" + other_phone,
                    row.getString(2) + " " + row.getString(3), Integer.valueOf(row.getString(4))));
        }
        arangoDB.shutdown();
    }


    /**
     * 绘制顶点
     *
     * @param vertex
     * @return
     * @throws ArangoDBException
     */
    private static VertexEntity createVertex(final Node vertex) throws ArangoDBException {
        return db.graph(GRAPH_NAME).vertexCollection(VERTEXT_COLLECTION_NAME).insertVertex(vertex);
    }

    /**
     * 绘制边
     *
     * @param edge
     * @return
     * @throws ArangoDBException
     */
    private static EdgeEntity saveEdge(final NodeEdge edge) throws ArangoDBException {
        return db.graph(GRAPH_NAME).edgeCollection(EDGE_COLLECTION_NAME).insertEdge(edge);
    }
}
