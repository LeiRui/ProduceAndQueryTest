package main;

import basic.Query;
import basic.RowData;
import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Insert;
import com.kenai.jffi.Array;
import distribution.DiscreteUniform;
import table.DiscreteUniformTable;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.*;

import com.datastax.driver.core.querybuilder.QueryBuilder;

/*
   给定一个查询和一张表格参数，
   要求(1)计算出查询seek代价公式理论结果；
   (2)根据表格参数生成数据并写入cassandra，然后统计实际查询耗时
 */
public class Cost {
    //public static String ks = "Cost_next";//TODO care about drop ks
    private static String nodes = "127.0.0.1";
    public String ks;
    public String cf;

    public Query query;
    public DiscreteUniformTable discreteUniformTable;
    public int CKn;
    public Cost(Query query, DiscreteUniformTable discreteUniformTable, String ks, String cf) {
        this.query = query;
        this.discreteUniformTable = discreteUniformTable;
        this.CKn=discreteUniformTable.CKn;// TODO:这里query can be null initially
        this.ks = ks;
        this.cf = cf;
    }

    public double QueryFormula(PrintWriter pw){
        double res = 0;

        //从query中提取目标点，然后分段计算，利用getBefore、getBetween计算seek路径上的无关点数的概率
        int qi = query.qi;
        double r1=query.r1;
        double r2=query.r2;

        double []p1 = query.CKPoints;
        p1[qi] = r1;

        RowData previous = null;
        RowData current = null;
        DiscreteUniform discreteUniform = discreteUniformTable.CKdist.get(qi);
        double pos = discreteUniform.start;
        for(int i=0; i<discreteUniform.number; i++, pos+=discreteUniform.delta) {
            //遍历离散等间距分布范围内的所有离散点，与range查询范围比较
            double lc = pos-r1;
            double rc = pos-r2;
            if(query.lisclosed) {
                if(lc < 0)
                    continue;//不满足range查询范围
            }
            else {
                if (Math.abs(lc - 0.0) <= 0.000001)
                    continue;//不满足range查询范围
                if (lc < 0)
                    continue;//不满足range查询范围
            }

            if(query.risclosed) {
                if(rc > 0)
                    continue;//不满足range查询范围
            }
            else {
                if (Math.abs(rc - 0.0) <= 0.000001)
                    continue;//不满足range查询范围
                if (rc > 0)
                    continue;//不满足range查询范围
            }

            //到这里的pos就是满足range查询范围的
            //double []ppre = query.CKPoints;
            //ppre[qi] = pos;
            double []p = query.CKPoints;
            p[qi] = pos;
            if(previous == null) {
                double [] pre=new double[p.length];
                System.arraycopy(p, 0, pre, 0, p.length);
                previous = new RowData(CKn, pre);
                res+=discreteUniformTable.getBefore(previous,pw);
                //System.out.println("res="+res*64);
                //System.out.println("res = "+res*rows);
            }
            else {
                current = new RowData(CKn,p);
                //System.out.println("previous:"+previous.data[qi]+",current:"+current.data[qi]);
                res += discreteUniformTable.getBetween(previous,current,qi,pw);
                //System.out.println("res="+res*64);
                //System.out.println("res = "+res*rows);
                double [] pre=new double[p.length];
                System.arraycopy(p, 0, pre, 0, p.length);
                previous.data=pre;
            }
        }
        return res;
    }


    /**
     * 按照表格描述信息，生成数据并写到Cassandra中
     * 注意：前面都没有考虑partition key，写到cassandra的时候，记得加上第一列的partition key
     * @param RNum  设想的行数
     * @return 实际生成去重后的行数
     * @throws Exception
     */
    public int insertTable(int RNum) throws Exception{//TODO 本来在table类中定义了rows，但其实不用，在实际生成数据的时候再考虑满足到规定的行数即可。当然可能有去重、理论计算高估的问题
        // 按照各列的分布函数产生数据
        double[][] table = new double[RNum][];
        Set<RowData> rowTable = new HashSet(); // no duplicates
        for(int i=0; i<RNum;i++) {
            table[i] = new double[CKn];
            for(int j=0; j<CKn; j++) {
                //按照每一列的分布函数随机产生值
                table[i][j] = discreteUniformTable.CKdist.get(j).getValue();
            }
            RowData rowdata =new RowData(CKn, table[i]);
            rowTable.add(rowdata);
        }

        //按照CKSet主次排序，以行为单位重新排列数据
        List sortedTable = new ArrayList(rowTable);
        Collections.sort(sortedTable);

        //把排序后的数据写入Cassandra
        String createCf = "CREATE TABLE IF NOT EXISTS " + ks + "." + cf + " (" + "rk int,";
        for(int i=1;i<=CKn;i++) {
            createCf = createCf+"c"+i+" double,";
        }
        createCf = createCf+"v int, PRIMARY KEY(rk,";
        for(int i =1; i<CKn;i++) {
            createCf=createCf+"c"+i+",";
        }
        createCf=createCf+"c"+CKn+"));";
        System.out.println(createCf);

        CassandraCluster cluster = CassandraCluster.getInstance(nodes);
        cluster.dropKeyspace(ks);
        cluster.createKeyspace(ks, "SimpleStrategy", 1); // no USE is ok???
        Session session = cluster.getSession();
        session.execute(createCf);

        System.out.println("write");
        Random random=new Random();
        Iterator iter = sortedTable.iterator();
        while(iter.hasNext()) {
            RowData rowdata = (RowData) iter.next();
            Insert tmp = QueryBuilder.insertInto(ks, cf).value("rk", 1);
            for(int i=1;i<=CKn;i++) {
                tmp=tmp.value("c"+i,rowdata.data[i-1]);
            }
            tmp=tmp.value("v", random.nextInt(100));
            Statement statement = tmp;
            session.execute(statement);
        }
        session.close();
        cluster.close();
        return rowTable.size();//去重后的行数
        /*
        PrintWriter pw1 = null;
        try {
            pw1 = new PrintWriter(new File(path1));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }


        Iterator iter = sortedTable.iterator();
        while(iter.hasNext()) {
            RowData rowdata = (RowData)iter.next();
            StringBuilder builder = new StringBuilder();
            for(int i=0;i<rowdata.CKn-1; i++){
                builder.append(rowdata.data[i]+",");
            }
            builder.append(rowdata.data[rowdata.CKn-1]);
            builder.append('\n');
            pw1.write(builder.toString());
        }
        pw1.close();
        //System.out.println("done!");
        */
    }


    /*
      Cassandra中的查询时间
     */
    public double QueryTime(){
        int rep = 10; // 重复执行查询取平均值减小随机波动误差
        Cluster cluster = Cluster.builder().addContactPoint(nodes).build();
        Session session = cluster.connect(ks);

        //String selectRangeCql1 = "select c1,c2,c3,c4  from " + ks + "." + cf + " WHERE c1 = 1 AND "
        //+"c2>=2 AND c2<=3 AND c3>=8 AND C3<=9 ALLOW FILTERING"; // note here don't select c5
        String selectRangeCql1="select rk,";
        for(int j=1;j<CKn; j++) {
            selectRangeCql1+=("c"+j+",");
        }
        selectRangeCql1= selectRangeCql1+"c"+CKn+" from "+ks+"."+cf+" WHERE rk=1 AND ";
        for(int j=0;j<CKn; j++) {
            if(j!=query.qi) {
                selectRangeCql1 = selectRangeCql1 + "c"+(j+1)+"="+query.CKPoints[j];
            }
            else {
                if(query.lisclosed){
                    selectRangeCql1=selectRangeCql1+"c"+(j+1)+">="+query.r1;
                }
                else {
                    selectRangeCql1=selectRangeCql1+"c"+(j+1)+">"+query.r1;
                }
                selectRangeCql1+=" AND ";
                if(query.risclosed){
                    selectRangeCql1=selectRangeCql1+"c"+(j+1)+"<="+query.r2;
                }
                else {
                    selectRangeCql1=selectRangeCql1+"c"+(j+1)+"<"+query.r2;
                }
            }
            if(j!=CKn-1) {
                selectRangeCql1+=" AND ";
            }
            else {
                selectRangeCql1+=" ALLOW FILTERING";
            }
        }
        System.out.println(selectRangeCql1);

        double time=0;
        for (int i = 0; i < rep; i++) {//repetition
            long elapsed = System.nanoTime();
            ResultSet rs1 = session.execute(selectRangeCql1);
            elapsed = System.nanoTime() - elapsed;
            time += elapsed / (double) Math.pow(10, 9);
            //System.out.println(time);
        }
        time = time / rep * Math.pow(10, 3);//unit:ms
        session.close();
        cluster.close();

        return time;
    }
}
