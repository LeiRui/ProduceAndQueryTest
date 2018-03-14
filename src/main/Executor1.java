package main;

import basic.Query;
import distribution.DiscreteUniform;
import table.DiscreteUniformTable;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

/*
  启动
 */
public class Executor1 {
    public static void main(String[] args) throws Exception {
        //part 1
        int CKn=3;
        List<DiscreteUniform> CKdist = new ArrayList();
        for(int i=0;i<CKn;i++) {
            CKdist.add(new DiscreteUniform(1,1,4));
        }
        DiscreteUniformTable discreteUniformTable = new DiscreteUniformTable(CKn,CKdist);
        //Query query = new Query(3,1,2,3,true,false,new double[]{2,888,3});
        Query query=null;
        Cost cost = new Cost(query,discreteUniformTable,"Cost64","exp1");

        //part 2
        PrintWriter pw=null;
        try {
            pw = new PrintWriter(new FileOutputStream("exp1_.csv",true)); // append
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }


        //part 3
        int RNum = 64;
        int rows=cost.insertTable(RNum);

        //part 4
        double [] r1 = new double[]{1,1,1,1, 2,2,2, 3,3, 4};
        double [] r2 = new double[]{1,2,3,4, 2,3,4 ,3,4,  4};
        for(int k=0;k<r1.length;k++) {
            query = new Query(3, 0, r1[k], r2[k], true, true, new double[]{888, 2, 3});
            cost.query = query;
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < CKn; i++) {
                if (i != query.qi) {
                    builder.append("" + query.CKPoints[i]);
                } else {
                    if (query.lisclosed)
                        builder.append("[");
                    else
                        builder.append("(");
                    builder.append(query.r1 + "-" + query.r2);
                    if (query.risclosed)
                        builder.append("]");
                    else
                        builder.append(")");
                }
                if (i == query.CKn - 1)
                    builder.append(",");
                else
                    builder.append("&");
            }
            pw.write(builder.toString());
            double time = cost.QueryTime(); //ms
            pw.write("" + time + "," + RNum + "," + rows + ",,");
            System.out.println(cost.QueryFormula(pw) * RNum);
            pw.write("\n");
        }

        r1 = new double[]{1,1,1,1, 2,2,2, 3,3, 4};
        r2 = new double[]{1,2,3,4, 2,3,4 ,3,4,  4};
        for(int k=0;k<r1.length;k++) {
            query = new Query(3, 1, r1[k], r2[k], true, true, new double[]{2,888, 3});
            cost.query = query;
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < CKn; i++) {
                if (i != query.qi) {
                    builder.append("" + query.CKPoints[i]);
                } else {
                    if (query.lisclosed)
                        builder.append("[");
                    else
                        builder.append("(");
                    builder.append(query.r1 + "-" + query.r2);
                    if (query.risclosed)
                        builder.append("]");
                    else
                        builder.append(")");
                }
                if (i == query.CKn - 1)
                    builder.append(",");
                else
                    builder.append("&");
            }
            pw.write(builder.toString());
            double time = cost.QueryTime(); //ms
            pw.write("" + time + "," + RNum + "," + rows + ",,");
            System.out.println(cost.QueryFormula(pw) * RNum);
            pw.write("\n");
        }

        r1 = new double[]{1,1,1,1, 2,2,2, 3,3, 4};
        r2 = new double[]{1,2,3,4, 2,3,4 ,3,4,  4};
        for(int k=0;k<r1.length;k++) {
            query = new Query(3, 2, r1[k], r2[k], true, true, new double[]{2, 3,888});
            cost.query = query;
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < CKn; i++) {
                if (i != query.qi) {
                    builder.append("" + query.CKPoints[i]);
                } else {
                    if (query.lisclosed)
                        builder.append("[");
                    else
                        builder.append("(");
                    builder.append(query.r1 + "-" + query.r2);
                    if (query.risclosed)
                        builder.append("]");
                    else
                        builder.append(")");
                }
                if (i == query.CKn - 1)
                    builder.append(",");
                else
                    builder.append("&");
            }
            pw.write(builder.toString());
            double time = cost.QueryTime(); //ms
            pw.write("" + time + "," + RNum + "," + rows + ",,");
            System.out.println(cost.QueryFormula(pw) * RNum);
            pw.write("\n");
        }


        pw.close();


        /*
        DiscreteUniform year = new DiscreteUniform(1,1,2018);
        DiscreteUniform month = new DiscreteUniform(1,1,12);
        DiscreteUniform day = new DiscreteUniform(1,1,30);
        DiscreteUniform hour = new DiscreteUniform(1,1,60);
        DiscreteUniform min = new DiscreteUniform(1,1,60);
        DiscreteUniform sec = new DiscreteUniform(1,1,60);
        CKdist.add(year);
        CKdist.add(month);
        CKdist.add(day);
        CKdist.add(hour);
        CKdist.add(min);
        CKdist.add(sec);
        */
    }
}
