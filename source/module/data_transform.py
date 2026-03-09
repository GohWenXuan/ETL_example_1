import pandas as pd
import re
import numpy as np
import logging

# create logger for this module
module_logger = logging.getLogger(__name__)


def get_resale_identifier(df):
    """
    Create resale_identifier column with format:
    - First character: 'S'
    - Next 3 digits: First 3 digits of block (padded with leading zeros)
    - Next 2 digits: First 2 digits of avg_resale_price (grouped by month, town, flat_type)
    - Next 2 digits: Month of current entry (01-12)
    - Final character: First character of town

    Parameters:
    df: DataFrame
    
    Returns:
    DataFrame with avg_resale_price and resale_identifier
    """
   
    try:
        # Create avg_resale_price
        df['avg_resale_price'] = df.groupby(['month', 'town', 'flat_type'])['resale_price'].transform('mean')
        
        # 1. Extract and pad block digits
        def get_block_part(block):
           
            # Extract only digits
            digits = re.sub(r'\D', '', str(block))
           
            # Take first 3 digits and pad with leading zeros
            first_digits = digits[:3] if digits else ''
            
            return first_digits.zfill(3)
        
        # 2. Get first 2 digits of avg_resale_price
        def get_price_part(price):
            if pd.isna(price):
                return '00'
            # Convert to string, remove decimal, take first 2 digits
            price_str = str(int(price))  # Convert to integer first to remove decimal
            
            return price_str[:2].zfill(2) 
        
        # 3. Get month from date
        def get_month_part(month_date):
            if pd.isna(month_date):
                return '00'
            # Extract month as 2-digit string
            return f"{month_date.month:02d}"
        
        # 4. Get first character of town
        def get_town_char(town):
            if pd.isna(town) or town == '':
                return 'X'
            return str(town)[0].upper()
        
        # Apply all functions
        df['block_part'] = df['block'].apply(get_block_part)
        df['price_part'] = df['avg_resale_price'].apply(get_price_part)
        df['month_part'] = df['month'].apply(get_month_part)
        df['town_char'] = df['town'].apply(get_town_char)
        
        # Create the identifier
        df['resale_identifier'] = ('S' + 
                                        df['block_part'] + 
                                        df['price_part'] + 
                                        df['month_part'] + 
                                        df['town_char'])
        
        # Drop temp columns
        df = df.drop(['block_part', 'price_part', 'month_part', 'town_char'], axis=1, inplace=True)    
        
    except Exception as e:
        module_logger.error(f"Error creating resale identifier: {str(e)}")

