"""
Transformer for processing parameter values of loan account.
"""

from typing import Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, when, coalesce, first,
    array, arrays_zip, collect_list, struct, create_map, 
    map_from_entries, filter as filter_array, to_json, count
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
            
    
    @property
    def name(self) -> str:
        """Get transformer name."""
        return "sa_account_parameter_value" 