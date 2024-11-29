# flink-udfs

当前支持的 Flink UDF

| 函数名称         | 功能描述                                                                 | 输入参数       | 输出参数            |
|------------------|--------------------------------------------------------------------------|----------------|---------------------|
| GetFieldByIndex  | Flink 内置函数只支持通过名称获取组合类型（Row / Struct）中的某个 Field，不支持通过索引 获取 | Row, Index     | Object              |
| SortAndTruncate  | 支持对复杂类型（Array<Row>）内部元素的排序                                | Array<Row>     | Array<Row>          |
| Transpose        | 对复杂类型转置后输出                                                     | Array<Row>     | Array<Array<String>>|

