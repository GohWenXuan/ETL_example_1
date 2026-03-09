import pandas as pd
import logging
from typing import Tuple, List, Dict
import numpy as np

# create logger for this module
module_logger = logging.getLogger(__name__)


def validate_months(df, month_threshold_lower, month_threshold_upper):
    """
    Validate if months in dataframe are within specified range.
    
    Parameters:
    df: DataFrame with 'month' column (datetime format)
    month_threshold_lower: Lower bound month (datetime or string)
    month_threshold_upper: Upper bound month (datetime or string)
    
    Returns:
    DataFrame with invalid records and 'invalid_reasons' column
    """
    try:
        # Convert thresholds to datetime before checking threshold
        if isinstance(month_threshold_lower, str):
            month_threshold_lower = pd.to_datetime(month_threshold_lower)

        if isinstance(month_threshold_upper, str):
            month_threshold_upper = pd.to_datetime(month_threshold_upper)
        
        # Find rows outside the threshold range
        invalid_mask = (df['month'] < month_threshold_lower) | (df['month'] > month_threshold_upper)
        
        # Get invalid records and add reason column
        invalid_records = df[invalid_mask].copy()
        
        if not invalid_records.empty:
            invalid_records['invalid_reasons'] = 'invalid months'
        
        return invalid_records
        
    except Exception as e:
        module_logger.error(f"Error in month validation: {str(e)}")
        return pd.DataFrame()  # Return empty dataframe on error



def validate_town(df, known_towns):
    """
    Validate if towns in dataframe are in the list of known towns.
    
    Parameters:
    df: DataFrame with 'town' column
    known_towns: List of valid town names
    
    Returns:
    DataFrame with invalid records and 'invalid_reasons' column
    """
    try:      
        # Find rows where town is not in known_towns list
        # Using ~ to negate the condition (not in list)
        invalid_mask = ~df['town'].isin(known_towns)
        
        # Get invalid records and add reason column
        invalid_records = df[invalid_mask].copy()
        
        if not invalid_records.empty:
            invalid_records['invalid_reasons'] = 'invalid town'
        
        return invalid_records
        
    except Exception as e:
        module_logger.error(f"Error in town validation: {str(e)}")
        return pd.DataFrame()  # Return empty dataframe on error





def validate_price_per_sqm(df, price_threshold_lower=300, price_threshold_upper=15000):
    """
    Validate if price_per_sqm is within reasonable range.
    
    Parameters:
    df: DataFrame with 'price_per_sqm' column
    price_threshold_lower: Lower bound for price per sqm (default: 300) - For example
    price_threshold_upper: Upper bound for price per sqm (default: 15000) - For example
    
    Returns:
    DataFrame with invalid records and 'invalid_reasons' column
    """
    try:      
        # Find rows that does not meet threshold
        invalid_mask = (df['price_per_sqm'] < price_threshold_lower) | \
                       (df['price_per_sqm'] > price_threshold_upper)
        
        # Get invalid records
        invalid_records = df[invalid_mask].copy()
        
        if not invalid_records.empty:
            # Add reason based on whether price is too low or too high
            too_low_mask = invalid_records['price_per_sqm'] < price_threshold_lower
            too_high_mask = invalid_records['price_per_sqm'] > price_threshold_upper
            
            invalid_records['invalid_reasons'] = None
            invalid_records.loc[too_low_mask, 'invalid_reasons'] = f'price per sqm below {price_threshold_lower}'
            invalid_records.loc[too_high_mask, 'invalid_reasons'] = f'price per sqm above {price_threshold_upper}'
        
        return invalid_records
        
    except Exception as e:
        module_logger.error(f"Error in price per sqm validation: {str(e)}")
        return pd.DataFrame()  # Return empty dataframe on error



def validate_duplicates(df):
    """
    Check for duplicate records (excluding resale_price) and flag the one with lower price.
    
    Parameters:
    df: DataFrame with columns to check for duplicates (all columns except resale_price)
    
    Returns:
    DataFrame with invalid records (the lower-priced duplicate) and 'invalid_reasons' column
    """
    try:
        df_copy = df.copy()
        
        # Exclude 'resale_price' for duplicate checking
        duplicate_check_cols = [col for col in df_copy.columns if col != 'resale_price']
        
        # Find duplicate groups based on all columns except resale_price
        # Keep all duplicates (including first occurrence for comparison)
        duplicate_mask = df_copy.duplicated(subset=duplicate_check_cols, keep=False)
        
        if not duplicate_mask.any():
            return pd.DataFrame()  # No duplicates found
        
        # Get all duplicate records
        duplicate_records = df_copy[duplicate_mask].copy()
        
        # Identify which record in each duplicate group has the lower price
        invalid_mask = pd.Series(False, index=duplicate_records.index)
        
        # Group by the duplicate keys
        for _, group in duplicate_records.groupby(duplicate_check_cols):
            if len(group) > 1:  # If there are multiple records in this group
                # Find the index of the minimum price
                min_price_idx = group['resale_price'].idxmin()
                # Mark the record with minimum price as invalid
                invalid_mask.loc[min_price_idx] = True
                       
        # Get invalid records (the lower-priced duplicates)
        invalid_records = duplicate_records[invalid_mask].copy()
        
        if not invalid_records.empty:
            invalid_records['invalid_reasons'] = 'duplicate record with lower resale price'
        
        return invalid_records
        
    except Exception as e:
        module_logger.error(f"Error in duplicate validation: {str(e)}")
        return pd.DataFrame()  # Return empty dataframe on error
    


def validate_missing_values(df):
    """
    Check for rows with missing values in any column.
    
    Parameters:
    df: DataFrame to check for missing values
    
    Returns:
    DataFrame with rows containing missing values and 'invalid_reasons' column
    """
    try:
        # Find rows with any missing values
        invalid_mask = df.isna().any(axis=1)
        
        # Get invalid records
        invalid_records = df[invalid_mask].copy()

        if not invalid_records.empty:
            invalid_records['invalid_reasons'] = 'missing values'
        return invalid_records
        
    except Exception as e:
        module_logger(f"Error in missing values validation: {str(e)}")
        return pd.DataFrame()