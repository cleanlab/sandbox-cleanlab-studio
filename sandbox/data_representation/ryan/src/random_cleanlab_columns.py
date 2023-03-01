import pyspark.sql
import pyspark.sql.functions


def generate_random_cleanlab_columns(dataset_table: pyspark.sql.DataFrame, id_column: str, label_column: str) -> pyspark.sql.DataFrame:
    """Generates randomized cleanlab columns from provided dataset table."""
    cleanlab_cols = dataset_table.select(id_column, label_column)

    # set cleanlab suggetested label column (shuffled version of label column)
    shuffled_suggested_label = cleanlab_cols.select(label_column).withColumn("__rand", pyspark.sql.functions.rand()).orderBy("__rand")
    shuffled_suggested_label = shuffled_suggested_label.withColumnRenamed(label_column, "cleanlab_suggested_label").drop("__rand")
    cleanlab_cols = cleanlab_cols.join(shuffled_suggested_label)

    # set corrected label and cleanlab action columns to null
    cleanlab_cols = cleanlab_cols.withColumns({
        "cleanlab_corrected_label": pyspark.sql.functions.lit(None).cast("string"),
        "cleanlab_action": pyspark.sql.functions.lit(None).cast("string"),
    })

    # set label quality columnn to rand
    cleanlab_cols = cleanlab_cols.withColumn("cleanlab_label_quality", pyspark.sql.functions.rand())

    return cleanlab_cols


def generate_random_ood_columns(dataset_table: pyspark.sql.DataFrame, id_column: str) -> pyspark.sql.DataFrame:
    """Generates random OOD columns from provided dataset table."""
    ood_cols = dataset_table.select(id_column)

    # set OOD probability column
    ood_cols = ood_cols.withColumn(
        "cleanlab_ood_prob", pyspark.sql.functions.rand()
    )
    # set OOD flag column
    ood_cols = ood_cols.withColumn(
        "cleanlab_ood_flag", pyspark.sql.functions.col("cleanlab_ood_prob") >= 0.6
    )

    return ood_cols
