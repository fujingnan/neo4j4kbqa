# neo4j_graph


+ 执行import_company_data.py脚本，可把company_data下面的数据导入到neo4j
+ 先执行stock_data，用stock_data下原始数据构建出导入图谱格式的数据，再执行import_stock_data.py脚本，把stock_data下所构建出的格式化数据导入到neo4j


+ gnn/saint.py脚本用于进行节点分类，该功能仍在探索优化中...