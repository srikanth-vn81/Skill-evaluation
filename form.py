import streamlit as st
import boto3
import pandas as pd
from io import StringIO

# Read AWS credentials from Streamlit secrets
aws_access_key_id = st.secrets["default"]["AKIAQ3EGWJGVAXZPBIXU"]
aws_secret_access_key = st.secrets["default"]["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"]

# Initialize the S3 client with credentials
s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

# Function to read existing data from S3
def read_csv_from_s3(bucket_name, s3_file_name):
    try:
        obj = s3.get_object(Bucket=bucket_name, Key=s3_file_name)
        data = obj['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(data))
        return df
    except s3.exceptions.NoSuchKey:
        return pd.DataFrame()  # Return an empty DataFrame if file does not exist

# Function to upload data to S3
def upload_to_s3(file_content, bucket_name, s3_file_name):
    try:
        s3.put_object(Body=file_content, Bucket=bucket_name, Key=s3_file_name)
        st.success(f"Data uploaded successfully to S3: {s3_file_name}")
    except Exception as e:
        st.error(f"Error uploading data: {e}")

# Streamlit app
st.title("Form Data to S3")

# Create a form
with st.form(key='user_form'):
    name = st.text_input("Name (Var Char)")
    emp_id = st.text_input("Emp ID (String)")
    machines_handled = st.selectbox("Machines Handled", ["SNLS", "DNLS", "Overlock", "Special Machines"])
    rating = st.selectbox("Rating", ["A+", "A", "B", "C", "D"])
    submit_button = st.form_submit_button(label='Submit')

# Handle form submission
if submit_button:
    # Create a DataFrame with the form data
    new_data = {'Name': [name], 'Emp ID': [emp_id], 'Machines Handled': [machines_handled], 'Rating': [rating]}
    new_df = pd.DataFrame(new_data)

    # Define S3 parameters
    bucket_name = 'epm-uniqlo'
    s3_file_name = 'form_data.csv'

    # Read existing data from S3
    existing_df = read_csv_from_s3(bucket_name, s3_file_name)

    # Append new data to existing data
    updated_df = pd.concat([existing_df, new_df], ignore_index=True)

    # Convert updated DataFrame to CSV
    csv_buffer = StringIO()
    updated_df.to_csv(csv_buffer, index=False)
    csv_content = csv_buffer.getvalue()

    # Upload updated CSV to S3
    upload_to_s3(csv_content, bucket_name, s3_file_name)
