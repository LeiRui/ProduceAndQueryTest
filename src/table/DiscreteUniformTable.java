package table;

import basic.RowData;
import distribution.DiscreteUniform;

import java.io.PrintWriter;
import java.util.List;

/*
   描述一个CKSet表格类的信息包括：
   总列数（即排序键的数量）、列的先后排序、各列的分布函数

   特别地，本类中各列均为离散等间距均匀分布，即各列的分布函数均是DiscreteUniform类
 */
public class DiscreteUniformTable extends CKTable {

    public DiscreteUniformTable(int CKn, List<DiscreteUniform> CKdist){
        this.CKn=CKn;
        this.CKdist=CKdist;
    }

    public double getBefore(RowData point,PrintWriter pw){
        int CKn=point.CKn;
        double []data = point.data;
        double res=0;
        double pre=1;
        for(int i=0;i< CKn; i++) {
            double r = CKdist.get(i).getLeftRangeExclude(data[i]);
            if(i>0){
                pre *= CKdist.get(i-1).getPoint(data[i-1]);
            }
            double x = pre*r;
            res = res + x;
            //pw.write(""+x*rows+",");
            pw.write(""+x+",");
        }
        return res;
    }

    public double getBetween(RowData p1, RowData p2, int qi, PrintWriter pw){
        int CKn = p1.CKn;
        double r1 = p1.data[qi];
        double r2 = p2.data[qi]; // 要求r1和r2在该qi列是离散相邻分布的，并且p1在前

        double res = 0;
        double h=1;
        for(int i=0;i<qi; i++) { // note:这里的qi是从0开始计的，因此不要用qi-1
            h *= CKdist.get(i).getPoint(p1.data[i]);
        }

        double h1=h*CKdist.get(qi).getPoint(p1.data[qi]);
        String tmp = "";
        for(int i=qi+1; i<CKn; i++) {
            double x =  h1*CKdist.get(i).getRightRangeExclude(p1.data[i]);
            res += x;
            h1 *= CKdist.get(i).getPoint(p1.data[i]);
            //pw.write(""+x*rows+",");
            tmp=""+x+","+tmp; // NOTE:here tmp is used for flipping the writing sequence

        }
        pw.write(tmp);

        double h2=h*CKdist.get(qi).getPoint(p2.data[qi]);
        for(int i=qi+1; i<CKn; i++) {
            double x = h2*CKdist.get(i).getLeftRangeExclude(p1.data[i]);
            res += x;
            h2*= CKdist.get(i).getPoint(p1.data[i]);
            //pw.write(""+x*rows+",");
            pw.write(""+x+",");
        }
        return res;
    }
}
