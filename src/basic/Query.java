package basic;

public class Query {
    public int CKn;

    /*
      range query的列位置（其余点查询），对应排序后的从0开始的第i位
     */
    public int qi;
    /*
      range query的左端点
     */
    public double r1;
    /*
      range query的左端点的开闭
     */
    public boolean lisclosed;
    /*
      range query的右端点
    */
    public double r2;
    /*
      range query的右端点的开闭
    */
    public boolean risclosed;

    /*
      数组长度位CKn，每一位代表对应列点查询的数值，其中第qi位随便赋值，因为第qi位是range query
     */
    public double[] CKPoints;

    public Query(int CKn, int i, double r1, double r2, boolean l, boolean r, double[] ckpoints) {
        this.CKn=CKn;
        this.qi=i;
        this.r1=r1;
        this.r2=r2;
        this.lisclosed=l;
        this.risclosed=r;
        CKPoints = ckpoints;
    }

    /*
      给一个数据点（即一行数据），判断这个数据点是否满足查询过滤条件
     */
    public boolean fit(RowData rowData){
        if(rowData.CKn!=CKn){// 这里自己注意不同CKn的不要比较，暂省抛异常
            System.out.println("please check CKn is equal");
            return false;
        }
        double lc = rowData.data[qi]-r1;
        if(lisclosed) {
            if(lc<0)
                return false;
        }
        else {
            if(Math.abs(lc-0.0) <= 0.000001)//区间左开的话，相等时应该判断false
                return false;
            if(lc<0)
                return false;
        }

        double rc = rowData.data[qi]-r2;
        if(risclosed) {
            if(rc>0)
                return false;
        }
        else {
            if(Math.abs(rc-0.0) <= 0.000001)//区间右开的话，相等时应该判断false
                return false;
            if(rc>0)
                return false;
        }

        for(int j=0;j<CKn;j++){
            if(j==qi)
                continue;
            double c = CKPoints[j]-rowData.data[j];
            if(Math.abs(c-0.0) > 0.000001){ // 点查询不等的话，判断false
                return false;
            }
        }
        return true;
    }

}
