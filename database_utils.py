# Example usage:
connector = DatabaseConnector()
connector.connect_to_database()

# Assuming cleaned_card_data is obtained from previous steps
cleaned_card_data = cleaning.clean_card_data(card_data)
connector.upload_to_db(cleaned_card_data, 'dim_card_details')

connector.close_connection()



