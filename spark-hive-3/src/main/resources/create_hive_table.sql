--新建分区表
create table if not exists ods_gantry_transaction(
                                                     GantryId string COMMENT '门架编号',
                                                     MediaType string COMMENT '收费介质类型',
                                                     TransTime timestamp COMMENT '交易时间',
                                                     PayFee int COMMENT '收费金额',
                                                     SpecialType string COMMENT '交易特情类型'
) partitioned by(month string)
STORED AS ORC;