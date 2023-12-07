import pandas as pd

class DataCleaning:
    def __init__(self):
       
        pass

    def clean_csv_data(self, data):
        
        cleaned_data = data.dropna()
        return cleaned_data

    def clean_api_data(self, data):
        
        cleaned_data = {'key': data['value']}
        return cleaned_data

    def clean_s3_data(self, data):
     
        cleaned_data = data.decode('utf-8')
        return cleaned_data


#%% step 6 

import pandas as pd

class DataCleaning:
    def __init__(self):
        # You may initialize any common parameters or configurations here.
        pass

    def clean_user_data(self, user_data):
        """
        Cleans the user data.

        Parameters:
        - user_data (pd.DataFrame): The user data to be cleaned.

        Returns:
        - pd.DataFrame: The cleaned user data.
        """
        cleaned_data = user_data.copy()

        # Handle NULL values
        cleaned_data = cleaned_data.dropna()

        # Check date formats and handle date-related errors
        try:
            cleaned_data['birth_date'] = pd.to_datetime(cleaned_data['birth_date'], errors='raise')
        except ValueError as e:
            print(f"Error converting 'birth_date' to datetime: {e}")
            # Handle the error as needed, e.g., fill with a default date or drop the problematic rows
            cleaned_data = cleaned_data[cleaned_data['birth_date'].notna()]

        # Correct data types
        cleaned_data['age'] = cleaned_data['age'].astype(int, errors='ignore')

        # Remove rows with incorrect information (e.g., age less than 0)
        cleaned_data = cleaned_data[cleaned_data['age'] >= 0]

        # Additional cleaning steps as needed

        return cleaned_data



# %%

import pandas as pd

class DataCleaning:
    def __init__(self):
        # You may initialize any common parameters or configurations here.
        pass

    def clean_card_data(self, card_data):
       
        
        cleaned_data = card_data.copy()

        cleaned_data = cleaned_data.dropna()
        
        cleaned_data['card_number'] = cleaned_data['card_number'].apply(self.clean_card_number)

       

        return cleaned_data

    def clean_card_number(self, card_number):
        
        # Example: Remove spaces and non-numeric characters
        #cleaned_number = ''.join(char for char in card_number if char.isdigit())
        #return cleaned_number


# %%

import pandas as pd

class DataCleaning:
    def __init__(self):
        # You may initialize any common parameters or configurations here.
        pass

    def _clean_store_data(self, store_data):
        """
        Cleans the store data.

        Parameters:
        - store_data (pd.DataFrame): The store data to be cleaned.

        Returns:
        - pd.DataFrame: The cleaned store data.
        """
        cleaned_data = store_data.copy()

        cleaned_data = cleaned_data.dropna()

        return cleaned_data



