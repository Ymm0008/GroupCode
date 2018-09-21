数据说明：

数据包含三部分，分别为商品基本信息数据(文本、图像)、用户历史行为数据和搭配套餐数据。


下面分别介绍三份数据：

提供上万套餐，十万级商品及图像，百万级用户，千万级行为的数据.


(1) 搭配套餐数据：dim_fashion_match_sets

列名          类型          含义                                         示例
 
coll_id      bigint      搭配套餐ID                                      1000

item_list    string     搭配套餐中的商品ID列表（分号分隔，    1002,1003,1004;439201;1569773,234303;19836
                         每个分号下可能会有不只一个商品，
                         后面为可替换商品，逗号分隔）


(2) 商品信息表：dim_items

列名          类型          含义                                         示例

item_id      bigint        商品ID                                       439201

cat_id       bigint        商品所属类目ID                                16

terms        string        商品标题分词后的结果，顺序被打乱             5263,2541,2876263

img_data     string        商品图片（注：初赛图片直接以文件形式提供，
                           图片文件命名为item_id.jpg,表中无该字段）

 
(3) 用户历史行为表：user_bought_history

列名          类型          含义                                         示例

user_id       bigint        用户ID                                     62378843278

item_id       bigint        商品ID                                      439201

create_at     string        行为日期（购买）                            20140911


注：上述表格中的用户ID,商品ID，类目ID 均经过脱敏处理；初赛脱敏后商品图片以文件形式提供(见all_imgs文件夹)，复赛脱敏后图片保存在ODPS字段中。



