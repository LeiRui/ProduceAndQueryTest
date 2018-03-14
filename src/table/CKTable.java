package table;

import basic.RowData;
import distribution.DiscreteUniform;

import java.io.PrintWriter;
import java.util.List;

/*
   CK: clustring keys

   描述一个CKSet表格类的信息包括：总列数（即排序键的数量）、列的先后排序、各列的分布函数，
   此外定义两个通用概率函数接口
 */
abstract public class CKTable {
    /*
        排序键的个数
     */
    public int CKn;

    /*
        排序键的先后排列顺序、排序键对应列的分布
     */
    public List<DiscreteUniform> CKdist;

    /*
       辅助formulate计算的函数，用于得到按照排序规则排在点point之前的概率,并通过pw记录中间的计算结果
    */
    abstract public double getBefore(RowData point, PrintWriter pw);


    /**
     * 辅助formulate计算的函数，用于得到按照排序规则排在两个点p1,p2之间的概率。
     * 其中，要求p1,p2是存在于离散点集中的点，并且p1,p2的qi列是相邻的前后两个
     * TODO：这里formulate计算时，似乎有默认点查询的点一定在离散集内，又好像没有限制，有空看一下
     * @param p1 第一个数据点（或者说表中的一行数据）
     * @param p2 第二个数据点（或者说表中的另一行数据）
     * @param qi range query的列
     * @param pw 记录中间计算结果的工具
     * @return
     */
    abstract public double getBetween(RowData p1, RowData p2, int qi,PrintWriter pw);
}
