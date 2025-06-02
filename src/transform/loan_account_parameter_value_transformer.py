"""
Transformer for processing parameter values of loan account.
"""

from typing import Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, when, coalesce, first,
    array, arrays_zip, collect_list, struct, create_map, 
    map_from_entries, filter as filter_array, to_json, count,
    date_format, sort_array, transform
)
from common.base import BaseTransformer
from common.utils.logging_utils import get_logger

logger = get_logger(__name__)

class LoanAccountParameterValueTransformer(BaseTransformer):
    """Transform parameter values of loan acocunt into a structured JSON format."""
    
    def __init__(self, options: Dict[str, Any]):
        """
        Initialize transformer.
        
        Args:
            options: Configuration options including:
                - param_type_map: Mapping of parameters to their types
        """
        super().__init__(options)
        self.logger = get_logger(__name__)
        
        if 'param_type_map' not in options:
            raise ValueError("param_type_map is required")
            
        self.param_type_map = options['param_type_map']
        
        self.logger.info(f"Initialized {self.name} transformer")

    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform parameter values DataFrame.
        
        Args:
            df: Input DataFrame loaded from parameter_values_loan source
                
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
        self.logger.info(f"Starting {self.name} transformation")
        parameter_values_loan = df  # Input is from parameter_values_loan source
        input_count = parameter_values_loan.count()
        self.logger.info(f"Processing {input_count} parameter value records")
        self.logger.info(f"Parameter values columns: {parameter_values_loan.columns}")

        auto_rollover_interest_schedule_plan_loan = kwargs.get('auto_rollover_interest_schedule_plan_loan')
        fixed_interest_rate_loan = kwargs.get('fixed_interest_rate_loan')
        original_due_interest_dates_loan = kwargs.get('original_due_interest_dates_loan')
        original_due_pincipal_records_loan = kwargs.get('original_due_principal_records_loan')
        
        if auto_rollover_interest_schedule_plan_loan is None:
            self.logger.error("auto_rollover_interest_schedule_plan_loan not provided in kwargs")
            raise ValueError("auto_rollover_interest_schedule_plan_loan is required in kwargs")
        
        if fixed_interest_rate_loan is None:
            self.logger.error("fixed_interest_rate_loan not provided in kwargs")
            raise ValueError("fixed_interest_rate_loan is required in kwargs")
            
        if original_due_interest_dates_loan is None:
            self.logger.error("original_due_interest_dates_loan not provided in kwargs")
            raise ValueError("original_due_interest_dates_loan is required in kwargs")
            
        if original_due_pincipal_records_loan is None:
            self.logger.error("original_due_pincipal_records_loan not provided in kwargs")
            raise ValueError("original_due_pincipal_records_loan is required in kwargs")
        
        auto_rollover_interest_schedule_plan_loan = auto_rollover_interest_schedule_plan_loan.groupBy("parameter_values_id").agg(
            to_json(transform(sort_array(collect_list(struct(
                "update_date",
                "base_rate_code",
                "spread_rate"
            ))), 
            lambda x: struct(
                x["base_rate_code"].alias("base_rate_code"),
                x["spread_rate"].alias("spread_rate"),
                date_format(x["update_date"], "yyyy-MM-dd").alias("update_date")
            ))).alias("auto_rollover_interest_schedule_plan_loan")
        )

        fixed_interest_rate_loan = fixed_interest_rate_loan.groupBy("parameter_values_id").agg(
            to_json(first(struct(
                "update_date",
                "base_rate_code",
                "spread_rate"
            ))).alias("fixed_interest_rate_loan")
        )

        original_due_interest_dates_loan = original_due_interest_dates_loan.groupBy("parameter_values_id").agg(
            to_json(sort_array(collect_list(struct(
                date_format(col("due_date"), "yyyy-MM-dd"),
            )))).alias("original_due_interest_dates_loan")
        )

        original_due_principal_records_loan = original_due_principal_records_loan.groupBy("parameter_values_id").agg(
            to_json(transform(sort_array(collect_list(struct(
                col("original_due_principal_date").alias("original_due_principal_date"),
                col("due_principal_amount").alias("due_principal_amount")
            ))), 
            lambda x: struct(
                x["due_principal_amount"].alias("due_principal_amount"),
                date_format(x["original_due_principal_date"], "yyyy-MM-dd").alias("original_due_principal_date")
            ))).alias("original_due_principal_records_loan")
        )

        join_df = parameter_values_loan.join(
            auto_rollover_interest_schedule_plan_loan,
            on="parameter_values_id",
            how="left"
        ).join(
            fixed_interest_rate_loan,
            on="parameter_values_id",
            how="left"
        ).join(
            original_due_interest_dates_loan,
            on="parameter_values_id",
            how="left"
        ).join(
            original_due_principal_records_loan,
            on="parameter_values_id",
            how="left"
        )

        self.agg_fields = [
            ("auto_rollover_interest_schedule_plan_loan", "auto_rollover_interest_schedule_plan_loan"),
            ("fixed_interest_rate_loan", "fixed_interest_rate_loan"),
            ("original_due_interest_dates_loan", "original_due_interest_dates_loan"),
            ("original_due_principal_records_loan", "original_due_principal_records_loan")]
            

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
            
        for field, col_name in self.agg_fields:
            outer_keys.append(lit(field))
            default_value = lit("[]") if col_name != "fixed_interest_rate_loan" else lit("{}")
            outer_values.append(create_map(lit("string_value"), coalesce(col(col_name), default_value)))
        
        # Create final parameter values JSON
        self.logger.info("Creating final parameter values JSON")
        zipped = arrays_zip(array(*outer_keys), array(*outer_values))
        filtered = filter_array(zipped, lambda x: x["0"].isNotNull())
        
        result_df = join_df.withColumn(
            "parameter_values",
            to_json(map_from_entries(filtered))
        ).select(
            "account_id",
            "parameter_values"
        )

        output_count = result_df.count()
        self.logger.info(f"Transformation complete. Output record count: {output_count}")
        
        return result_df
        
        
    @property
    def name(self) -> str:
        """Get transformer name."""
        return "loan_account_parameter_value" 