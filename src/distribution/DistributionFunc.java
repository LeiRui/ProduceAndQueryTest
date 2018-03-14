package distribution;

/*
    离散分布函数的一些函数接口
 */
interface DistributionFunc {

    /*
      计算落在某一点的概率
    */
    double getPoint(double point);

    /*
       计算小于点point的概率,不包括点point
     */
    double getLeftRangeExclude(double point);

    /*
       计算大于点point的概率,不包括点point
     */
    double getRightRangeExclude(double point);

    /*
       根据分布函数，随机返回一个值
     */
    double getValue();
}
