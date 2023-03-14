# Databricks notebook source
# MAGIC %run ./setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### MERCHANTS_AUD filtrando por idFeePlan_MOD=1 o idPlanLimit_MOD=1

# COMMAND ----------

MA_filtrado = (spark.table('bronze.merchants_aud')
      .filter((col('idFeePlan_MOD') == 1) | (col('idPlanLimit_MOD') == 1))
      .select('*')
     )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Crea vista temporal

# COMMAND ----------

spark.table('bronze.merchants_aud').createOrReplaceTempView('merchants_aud')
spark.table('bronze.merchants').createOrReplaceTempView('merchants')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Subconsulta para obtener la REV máxima para cada ID_MERCHANT

# COMMAND ----------

X = spark.sql("""
    SELECT MA.ID_MERCHANT, MAX(MA.REV) AS M_REV
    FROM merchants_aud MA
    INNER JOIN bronze.rev_info RI
        ON RI.REV = MA.REV
    WHERE idFeePlan_MOD=1 OR idPlanLimit_MOD=1 OR MA.REVTYPE = 0
    GROUP BY MA.ID_MERCHANT
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Subconsulta AP

# COMMAND ----------

AP = spark.sql("""
    SELECT M.ID_MERCHANT,
         IF(MAF.AFFECTS_ACCOUNT=1,
         "Agregador",
         IF(MAF.AFFECTS_ACCOUNT=0,
         "Procesador" ,
         NULL)) TIPO ,
         MAF.AFFECTS_ACCOUNT
                FROM MERCHANTS M
                JOIN bronze.merchants_affiliations MAF ON M.ID_MERCHANT=MAF.ID_MERCHANT
                WHERE M.CLASSIFICATION <>'EGLOBAL'
                GROUP BY  M.ID_MERCHANT,MAF.AFFECTS_ACCOUNT
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Subconsulta FF

# COMMAND ----------

FF = spark.sql("""
    SELECT AP.ID_MERCHANT,
         MAX(AP.AFFECTS_ACCOUNT) max_afecta_cuenta
            FROM 
                (SELECT M.ID_MERCHANT,
         IF(MA.AFFECTS_ACCOUNT=1,
         "Agregador",
         IF(MA.AFFECTS_ACCOUNT=0,
         "Procesador" ,
         NULL)) TIPO ,
         MA.AFFECTS_ACCOUNT
                FROM MERCHANTS M
                JOIN bronze.merchants_affiliations MA ON M.ID_MERCHANT=MA.ID_MERCHANT
                WHERE M.CLASSIFICATION <>'EGLOBAL'
                GROUP BY  M.ID_MERCHANT,MA.AFFECTS_ACCOUNT ) AP
                        GROUP BY  AP.ID_MERCHANT
""")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Master Joins

# COMMAND ----------

Master_join = (spark.table('bronze.merchants').alias('M')
     .join(MA_filtrado.alias('MA'), col('M.ID_MERCHANT') == col('MA.ID_MERCHANT'), 'left')
     .join(spark.table('bronze.merchants_info').alias('MI'), col('M.ID_MERCHANT_INFO') == col('MI.ID_MERCHANT_INFO'), 'left')
     .join(spark.table('bronze.fee_plans').alias('FP'), 
           ((col('FP.ID_FEE_PLAN') == col('MA.ID_FEE_PLAN')) | 
            ((col('FP.ID_FEE_PLAN') == col('M.ID_FEE_PLAN')) & (col('MA.REV').isNull()))),
           'left')
     .join(spark.table('bronze.fee_plan_costs').alias('FPC'),
           ((col('FPC.ID_FEE_PLAN') == col('MA.ID_FEE_PLAN')) | 
            ((col('FPC.ID_FEE_PLAN') == col('M.ID_FEE_PLAN')) & (col('MA.REV').isNull()))),
           'left')
     .join(spark.table('bronze.rev_info').alias('RI'), col('MA.REV') == col('RI.REV'), 'left')
     .join(X.alias('X'), col('X.ID_MERCHANT') == col('MA.ID_MERCHANT'), 'left')
     .join(FF.alias('FF'), col('FF.ID_MERCHANT') == col('MA.ID_MERCHANT'), 'left')
     .select('M.*', 'MA.*', 'FP.*','FPC.*','RI.*','X.M_REV','FF.max_afecta_cuenta','MI.*')
              )

# COMMAND ----------

display(Master_join)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Master Select

# COMMAND ----------

new_df =Master_join.select(
    md5(concat(col('M.ID_MERCHANT'), col('FPC.ID_FEE_PLAN_COSTS'), when(col('MA.REV').isNull(), 'NA').otherwise(col('MA.REV')))).alias('id_document_key'),
    col('FPC.ID_FEE_PLAN_COSTS').alias('plan_id_fee_plan_costs'),
    col('M.ID').alias('merchant_id'),
    col('M.ID_MERCHANT').alias('merchant_id_merchant'),
    col('M.NAME').alias('merchant_name'),
    col('MI.RFC').alias('merchant_rfc'),
    col('M.CREATION_DATE').alias('merchant_creation_date'),
    col('M.CLASSIFICATION').alias('merchant_classification'),
    col('MI.TRADENAME').alias('merchant_tradename'),
    col('M.COUNTRY').alias('merchant_country'),
    col('FP.ID_FEE_PLAN').alias('plan_id_fee_plan'),
    when((col('MA.REV') == col('X.M_REV')) | col('MA.REV').isNull(), 'PRODUCTIVO').otherwise('DEPRECATED').alias('plan_bi_status'),
    col('FP.NAME').alias('plan_name'),
    when(col('FPC.PERCENTAGE').isNull(), '-').otherwise((col('FPC.PERCENTAGE') * 100).cast('string')).alias('plan_str_percentage'),
    when(col('FPC.AMOUNT').isNull(), '-').otherwise(col('FPC.AMOUNT').cast('string')).alias('plan_str_fee_amount'),
    col('FPC.PERCENTAGE').alias('plan_percentage'),
    col('FPC.AMOUNT').alias('plan_fee_amount'),
    col('FPC.CURRENCY').alias('plan_currency'),
    when(col('FPC.OPERATOR').isNull(), '-').otherwise(col('FPC.OPERATOR')).alias('plan_operator'),
    when(col('FPC.CARD_TYPE').isNull(), '-').otherwise(col('FPC.CARD_TYPE')).alias('plan_card_type'),
    col('FPC.PAYMENT_TYPE').alias('plan_payment_type'),
    when(col('FPC.PAYMENTS').isNull(), '-').otherwise(col('FPC.PAYMENTS').cast('string')).alias('plan_msi'),
    col('FPC.AMOUNT_RULE').alias('plan_amount_rule'),
    col('FPC.FEES_FOR_FAILED').alias('plan_fee_for_failed'),
    col('FPC.STATUS').alias('plan_status'),
    when(col('FPC.APPLIES_TAX') == 0, 'No').otherwise('Si').alias('plan_applies_tax'),
    col('FPC.TRANSACTION_STATUS_CATEGORY').alias('plan_trx_status_category'),
    col('FPC.ID_TRANSACTION_TYPE').alias('plan_id_type'),
    when(col('FPC.ID_TRANSACTION_TYPE') == 3, 'Tarjeta')
    .when((col('FPC.ID_TRANSACTION_TYPE') == 16) & (col('M.COUNTRY') != 'COL'), 'Ventas Paynet')
    .when((col('FPC.ID_TRANSACTION_TYPE') == 26) & (col('M.COUNTRY') != 'COL'), 'Deposito Payet')
    .when((col('FPC.ID_TRANSACTION_TYPE').isin([16, 26])) & (col('M.COUNTRY') == 'COL'), 'Pago en tienda')
    .when(col('FPC.ID_TRANSACTION_TYPE') == 5, 'Pagos a Terceros')
    .when(col('FPC.ID_TRANSACTION_TYPE') == 15, 'Banco')
    .when((col('FPC.ID_TRANSACTION_TYPE').isin([5, 15])) & (col('M.COUNTRY') == 'COL'), 'PSE')
    .when((col('FPC.ID_TRANSACTION_TYPE') == 1) & (col('M.COUNTRY') == 'COL'), 'Transferencia')
    .when((col('FPC.ID_TRANSACTION_TYPE') == 1) & (col('M.COUNTRY') != 'COL'), 'SPEI Cuenta de banco')
    .when(col('FPC.ID_TRANSACTION_TYPE') == 36, 'Checkaut Lending')
    .when((col('FPC.ID_TRANSACTION_TYPE') == 18) & (col('M.COUNTRY') != 'COL'), 'Liquidación')
    .when((col('FPC.ID_TRANSACTION_TYPE') == 18) & (col('M.COUNTRY') == 'COL'), 'Liquidación Adicional')
    .when(col('FPC.ID_TRANSACTION_TYPE') == 13, 'Liquidación con Tarjeta')
    .when(col('FPC.ID_TRANSACTION_TYPE') == 28, 'Cargo Bitcoin')
    .when(col('FPC.ID_TRANSACTION_TYPE') == 33, 'Cargo Alipay')
    .when(col('FPC.ID_TRANSACTION_TYPE') == 12, 'Reembolso con Tarjeta')
    .otherwise('Otro')
    .alias('transaction_method_payment'),
        when(col('M.COUNTRY').isin(['CHL', 'JPN', 'GRC']), 'token')
        .when(col('FPC.ID_TRANSACTION_TYPE') == 1, 'spei')
        .when(col('FPC.ID_TRANSACTION_TYPE').isin([3, 12]), 
              when(col('FF.max_afecta_cuenta') == 1, 'psp')
              .otherwise(
                  when(col('M.CLASSIFICATION') == 'EGLOBAL', 'ecommerce')
                  .otherwise(
                      when(col('M.COUNTRY') == 'MEX', 'gateway')
                      .otherwise('token')
                  )
              )
        )
        .when(col('FPC.ID_TRANSACTION_TYPE').isin([5, 13, 15, 18]), 'spei')
        .when(col('FPC.ID_TRANSACTION_TYPE').isin([16, 23, 26]), 'paynet')
        .when(col('FPC.ID_TRANSACTION_TYPE') == 28, 'bitcoin')
        .otherwise('other')
        .alias('transaction_trx_product'),
       when(col('MA.REV').isNull(), '-').otherwise(col('MA.REV')).alias('merchant_aud_rev'),
       when(col('RI.USERNAME').isNull(), '-').otherwise(col('RI.USERNAME')).alias('plan_changeby'),
       when(col('MA.NAME').isNull(), '-').otherwise(col('MA.NAME')).alias('merchant_aud_name'),
       when(col('MA.EMAIL').isNull(), '-').otherwise(col('MA.EMAIL')).alias('merchant_aud_email'),
        col('MA.UPDATE_TIMESTAMP').alias('merchant_aud_modificationdate'),
       when(col('MA.CLASSIFICATION').isNull(), '-').otherwise(col('MA.CLASSIFICATION')).alias('merchant_aud_classification'),
       when(col('MA.MERCHANT_CATEGORY_CODES').isNull(), '-').otherwise(col('MA.MERCHANT_CATEGORY_CODES')).alias('merchant_aud_mcc'),
       when(col('MA.WEBSITE_URL').isNull(), '-').otherwise(col('MA.WEBSITE_URL')).alias('merchant_aud_websiteUrl'),
       when(col('MA.STATUS').isNull(), '-').otherwise(col('MA.STATUS')).alias('merchant_aud_status'),
        coalesce(col('MA.idPlanLimit_MOD'), lit('0')).alias('merchant_aud_idplansi_limitmod'),
        coalesce(col('MA.idFeePlan_MOD'), lit('0')).alias('merchant_aud_idfeeplanmod'),
       lit(1).alias('transaction_num_reg'),
col('M.STATUS').alias('merchant_status')
)
display(new_df)

# COMMAND ----------

new_df.count()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Silver Delta

# COMMAND ----------

new_df.write.format("delta").mode("overwrite").saveAsTable("poc_catalog.silver.result")
