import re
import PyPDF2
import pandas as pd
import mysql.connector
from mysql.connector import Error

# Function to extract text from PDF
def extract_text_from_pdf(pdf_path):
    text = ""
    with open(pdf_path, 'rb') as file:
        reader = PyPDF2.PdfReader(file)
        for page in reader.pages:
            text += page.extract_text() + "\n"
    return text

# Function to parse client details and transactions
def parse_transactions(text):
    # Extract client details
    client_details_pattern = re.compile(
        r"PAN of Client\s*:\s*(\w+)\s*"
        r".*?Contract Note No\s*:\s*(\w+)\s*"
        r".*?Settlement Date\s*:\s*(\d{2}/\d{2}/\d{4})",
        re.DOTALL  # Allows '.' to match newline characters
    )
    client_details = client_details_pattern.search(text)
    if not client_details:
        raise ValueError("Client details not found in the text.")

    pan_number = client_details.group(1)
    contract_note_number = client_details.group(2)
    settlement_date = client_details.group(3)

    # Regex to parse transactions
    transaction_pattern = re.compile(
        r"(\d{12})\s+(\d{2}:\d{2}:\d{2})\s+(\d{8})\s+(\d{2}:\d{2}:\d{2})\s+"
        r"([\w\s]+)-\s"
        r"(BUY|SELL)-(\d+)\s+(\d+\.\d{2})\s+(\d+\.\d+?)\s+(\d+\.\d+?)\s+([\d,]+\.\d{2})\sCr",
        re.MULTILINE
    )
    data = []
    for txn in transaction_pattern.findall(text):
        symbol_security = txn[4].strip()[:-3] if txn[4].strip().endswith('NSE') else txn[4].strip()

        data.append({
            'Order_No': txn[0],
            'PAN_Number': pan_number,
            'Date': settlement_date,
            'Order_Time': txn[1],
            'Contract_Note_Number': contract_note_number,
            'Symbol_Security': symbol_security,
            'Buy_Sell': txn[5],
            'Quantity': int(txn[6]),
            'Price': float(txn[7]),
            'Net_Total': float(txn[10].replace(',', ''))
        })
    return data

# Function to connect to MySQL and insert data
def insert_into_db(data, db_config):
    try:
        connection = mysql.connector.connect(**db_config)
        if connection.is_connected():
            cursor = connection.cursor()
            cursor.execute("CREATE DATABASE IF NOT EXISTS FINYANTRA")
            cursor.execute("USE FINYANTRA")
            # Security_Desc VARCHAR(255),
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS Transactions (
                    `Order_No` VARCHAR(255),
                    `PAN_Number` VARCHAR(255),
                    `Date` VARCHAR(255),
                    `Order_Time` TIME,
                    `Contract_Note_Number` VARCHAR(255),
                    `Symbol/Security` VARCHAR(255),
                    `Buy/Sell` VARCHAR(10),
                    `Quantity` INT,
                    `Price` DECIMAL(10, 2),
                    `Net_Total` DECIMAL(10, 2)
                );
            """)

            insert_query = """
                INSERT INTO Transactions (
                    `Order_No`,
                    `PAN_Number`,
                    `Date`,
                    `Order_Time`,                                   
                    `Contract_Note_Number`,
                    `Symbol/Security`,
                    `Buy/Sell`,
                    `Quantity`,
                    `Price`,
                    `Net_Total`
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """

            for item in data:
                cursor.execute(insert_query, list(item.values()))
            connection.commit()
            print("Data inserted successfully")
    except Error as e:
        print("Error while connecting to MySQL", e)
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

# Main execution
if __name__ == "__main__":
    pdf_path = 'iifl.pdf'
    db_config = {
        'host': 'localhost',
        'user': 'root',
        'password': 'root'
    }
    text = extract_text_from_pdf(pdf_path)
    transaction_data = parse_transactions(text)
    print(transaction_data)
    insert_into_db(transaction_data, db_config)