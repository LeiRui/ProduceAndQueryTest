package distribution;

import java.util.Random;

public class DiscreteUniform implements DistributionFunc {
    /*
     定义离散等间距均匀分布的三个描述参数：start, delta, number
     离散点均匀分布:start, start+delta, ..., start+(number-1)*delta,
     number>=1
      */
    public double start;//起点
    public double delta;//间距
    public int number;//段数
    public DiscreteUniform(double start, double delta, int number) {
        this.start = start;
        this.delta = delta;
        this.number = number;
    }
    public double getPoint(double point) {
        return (double)1/number;
    }

    public double getLeftRangeExclude(double point) {
        double res = 0;
        double x=start;
        double unit = (double)1/number;
        int count = 1;
        while(x<point && count <= number) {
            res += unit;
            x += delta;
            ++count;
        }
        return res;
    }

    public double getRightRangeExclude(double point) {
        double res = 0;
        double x=start;
        double unit = (double)1/number;
        int count = 1;
        while(count <= number) {
            if(x>point) {
                res+=unit;
            }
            x += delta;
            ++count;
        }
        return res;
    }

    public double getValue(){
        Random rand = new Random();
        int random = rand.nextInt(number)+1; // 生成[1,number]区间内的随机数
        double res = start + (random-1) * delta;
        return res;
    }




}
