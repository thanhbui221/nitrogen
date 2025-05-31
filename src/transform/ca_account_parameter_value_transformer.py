"""
Transformer for processing parameter values with blockades.
"""

from typing import Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, when, coalesce, first,
    array, arrays_zip, collect_list, struct, create_map, 
    map_from_entries, filter as filter_array, to_json, count
)
from common.base import BaseTransformer
from common.utils.spark_utils import to_plain_decimal
from common.utils.logging_utils import get_logger

logger = get_logger(__name__)

class CAAccountParameterValueTransformer(BaseTransformer):
    """Transform parameter values into a structured JSON format with blockades."""
    
    def __init__(self, options: Dict[str, Any]):
        """
        Initialize transformer.
        
        Args:
            options: Configuration options including:
                - columns_to_cast: List of decimal columns to fix
                - param_type_map: Mapping of parameters to their types
        """
        super().__init__(options)
        self.logger = get_logger(__name__)
        self.columns_to_cast = options.get('columns_to_cast')
        
        self.param_type_map = options.get('param_type_map')
        
        self.logger.info(f"Initialized {self.name()} transformer with {len(self.columns_to_cast)} decimal columns to cast")
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform parameter values DataFrame.
        
        Args:
            df: Input DataFrame loaded from parameter_values_ca source
                Expected columns:
                - id: Primary key for parameter values
                - margin_interest_rate (optional)
                - minimum_balance (optional)
                - minimum_balance_requirement (optional)
                - overdraft_original_limit (optional)
                
            kwargs: Additional arguments including:
                - blockade_ca: DataFrame loaded from blockade_ca source
                  Expected columns:
                  - parameter_values_id: Foreign key to parameter_values_ca.id
                  - blockade_id
                  - start
                  - end
                  - block_amount
            
        Returns:
            Transformed DataFrame with JSON parameter values
        """
        self.logger.info(f"Starting {self.name()} transformation")
        parameter_values_ca = df  # Input is from parameter_values_ca source
        input_count = parameter_values_ca.count()
        self.logger.info(f"Processing {input_count} parameter value records")
        self.logger.info(f"Parameter values columns: {parameter_values_ca.columns}")

        blockade_ca = kwargs.get('blockade_ca')
        if blockade_ca is None:
            self.logger.error("blockade_ca not provided in kwargs")
            raise ValueError("blockade_ca is required in kwargs")
            
        blockade_count = blockade_ca.count()
        self.logger.info(f"Found {blockade_count} blockade records")
        self.logger.info(f"Blockade columns: {blockade_ca.columns}")

        # Fix decimal columns in parameter values DataFrame first
        self.logger.info("Converting decimal columns to normalized string format")
        for column in self.columns_to_cast:
            if column in parameter_values_ca.columns:
                parameter_values_ca = parameter_values_ca.withColumn(
                    column,
                    to_plain_decimal(col(column))
                )
                null_count = parameter_values_ca.filter(col(column).isNull()).count()
                if null_count > 0:
                    self.logger.warning(f"Found {null_count} null values in column {column}")

        # Aggregate blockades into JSON array
        self.logger.info("Aggregating blockades into JSON array")
        blockade_struct = blockade_ca.groupBy("parameter_values_id").agg(
            to_json(collect_list(struct(
                col("blockade_id").cast("string").alias("blockade_id"),
                col("start").cast("string").alias("start"),
                col("end").cast("string").alias("end"),
                col("block_amount").cast("string").alias("block_amount")
            ))).alias("blockade_json")
        )
            
        # Join with blockades
        self.logger.info("Joining parameter values with blockades")
        df_with_blockades = parameter_values_ca.join(
            blockade_struct,
            parameter_values_ca["id"] == blockade_struct["parameter_values_id"],
            "left"
        )
        
        join_count = df_with_blockades.count()
        if join_count != input_count:
            self.logger.warning(f"Join resulted in {join_count} records (was {input_count})")
        
        # Build parameter value map
        self.logger.info("Building parameter value map")
        outer_keys = []
        outer_values = []
        
        for field, wrapper in self.param_type_map.items():
            if wrapper == "string_value":
                condition = col(field).isNotNull() & (col(field) != "")
            elif wrapper == "decimal_value":
                condition = col(field).isNotNull()
            else:
                condition = lit(True)
                
            outer_keys.append(
                when(condition, lit(field)).otherwise(None)
            )
            outer_values.append(
                when(condition, create_map(lit(wrapper), col(field)))
                .otherwise(None)
            )
            
        # Add blockade field
        outer_keys.append(lit("blockade"))
        outer_values.append(
            create_map(
                lit("string_value"),
                coalesce(col("blockade_json"), lit("[]"))
            )
        )
        
        # Create final parameter values JSON
        self.logger.info("Creating final parameter values JSON")
        zipped = arrays_zip(array(*outer_keys), array(*outer_values))
        filtered = filter_array(zipped, lambda x: x["0"].isNotNull())
        
        result_df = df_with_blockades.withColumn(
            "parameter_values",
            to_json(map_from_entries(filtered))
        ).select(
            # Keep id for joining with account data
            parameter_values_ca["id"].alias("account_id"),
            "parameter_values"
        )

        output_count = result_df.count()
        self.logger.info(f"Transformation complete. Output record count: {output_count}")
        
        return result_df
    
    @staticmethod
    def name() -> str:
        """Get transformer name."""
        return "ca_account_parameter_value" 