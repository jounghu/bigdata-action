## age


大数据Spark “蘑菇云”行动第76课： Kafka+Spark Streaming+Redis项目实战 - 段智华的博客 - CSDN博客

https://blog.csdn.net/duan_zhihua/article/details/53120644


https://blog.csdn.net/u010675669/article/details/81744386


```text
该案例中，我们将假设我们需要统计一个 1000 万人口的所有人的平均年龄，当然如果您想测试 Spark 对于大数据的处理能力，您可以把人口数放的更大，比如 1 亿人口，当然这个取决于测试所用集群的存储容量。假设这些年龄信息都存储在一个文件里，并且该文件的格式如下，第一列是 ID，第二列是年龄。
```

## sex

```text
本案例假设我们需要对某个省的人口 (1 亿) 性别还有身高进行统计，需要计算出男女人数，男性中的最高和最低身高，以及女性中的最高和最低身高。本案例中用到的源文件有以下格式, 三列分别是 ID，性别，身高 (cm)。
```