from flask import Flask, render_template,request, redirect, url_for
import boto3
import pandas as pd
import re
from io import StringIO

app = Flask(__name__)

csv_keys = ['Car_sales_transactions.csv','Sales_Status.csv','City_Master.csv','Postal_Code_Master.csv','Region_Master.csv','Region_State_Mapping.csv','State_Master.csv']
S3_BUCKET = 'usedcarsbucket'
S3_REGION = 'ap-south-1'

s3 = boto3.client('s3', region_name=S3_REGION,aws_access_key_id='AKIA4MTWL5QWEYOLYIXI',
                         aws_secret_access_key='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')

def get_csv_from_s3(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    data = obj['Body'].read().decode('utf-8')
    return pd.read_csv(StringIO(data))

@app.route('/')
def index():
    # List of CSV file keys
    dataframes = [get_csv_from_s3(S3_BUCKET, key) for key in csv_keys]
    #tables = [df.to_html(classes='data') for df in dataframes]
    #titles = [df.columns.values for df in dataframes]
    return render_template('index.html', tables=csv_keys)
@app.route('/show_table', methods=['GET','POST'])
def show_table():
    if request.method == 'POST':
        selected_table = request.form.get('table_name')
    else:  # GET request
        selected_table = request.args.get('table_name')
    #print(f"Selected table: {selected_table}")
    curr_data=get_csv_from_s3(S3_BUCKET,selected_table)
    #tables=curr_data.to_html(classes='data')
    columns=curr_data.columns.tolist()
    titles=curr_data.values.tolist()
    if selected_table not in csv_keys:
        print("Invalid table selection")  # Debugging line
        return "Invalid table selection", 400
    return render_template('index2.html', tables=columns,titles=titles,currtable=selected_table)
@app.route('/firstOwner', methods=['GET','POST'])
def firstOwner():
    curr_data=get_csv_from_s3(S3_BUCKET,'Car_sales_transactions.csv')
    filtered_data = curr_data[
    (curr_data['Owner'] == 'First Owner') &
    (curr_data['Year'].between(2016, 2020)) &
    (curr_data['km_Driven'] < 80000)]
    columns=filtered_data.columns.tolist()
    titles=filtered_data.values.tolist()
    return render_template('index2.html', tables=columns,titles=titles,currtable='Details of First Car Owners')
@app.route('/fuelDeasel', methods=['GET','POST'])
def dieselcars():
    curr_data=get_csv_from_s3(S3_BUCKET,'Car_sales_transactions.csv')
    def extract_numeric_mileage(mileage):
        match = re.search(r'^[0-9]+(\.[0-9]+)?', mileage)
        return float(match.group(0)) if match else None

    curr_data['NumericMileage'] = curr_data['Mileage'].apply(extract_numeric_mileage)
    filtered_data = curr_data[
    (curr_data['NumericMileage'].between(24, 26)) &
    (curr_data['Year'].between(2018, 2020)) &
    (curr_data['Seats'].isin([4, 5])) &
    (curr_data['Fuel'] == 'Diesel')]
    columns=filtered_data.columns.tolist()
    titles=filtered_data.values.tolist()
    return render_template('index2.html', tables=columns,titles=titles,currtable='Diesel cars data')
@app.route('/salesStatus', methods=['GET','POST'])
def unsoldcarsstatus():
    curr_data=get_csv_from_s3(S3_BUCKET,'Car_sales_transactions.csv')
    second_data=get_csv_from_s3(S3_BUCKET,'Sales_Status.csv')
    merged_data = pd.merge(curr_data,second_data, on='Sales_ID')
    filtered_data = merged_data[
    (merged_data['Sold'] == 'N') &
    (merged_data['Seller_Type'].isin(['Individual', 'Dealer'])) &
    (merged_data['km_Driven'] < 60000) &
    (merged_data['Year'].between(2014, 2020))]
    columns=filtered_data.columns.tolist()
    titles=filtered_data.values.tolist()
    return render_template('index2.html', tables=columns,titles=titles,currtable='Unsold Cars Data')
@app.route('/accordingCity',methods=['GET','POST'])
def cityaccording():
    first_data=get_csv_from_s3(S3_BUCKET,'Car_sales_transactions.csv')
    second_data=get_csv_from_s3(S3_BUCKET,'City_Master.csv')
    def extract_numeric_mileage(mileage):
        match = re.search(r'^[0-9]+(\.[0-9]+)?', mileage)
        return float(match.group(0)) if match else None
    first_data['NumericMileage'] = first_data['Mileage'].apply(extract_numeric_mileage)
    merged_data = pd.merge(first_data,second_data, on='City_Code')
    filtered_data = merged_data[
    (merged_data['Transmission'].isin(['Manual', 'Automatic'])) &
    (merged_data['NumericMileage'].between(20, 25)) &
    (merged_data['City_Name'].isin(['Mumbai', 'Hydrabad', 'Surat', 'Kanpur']))]
    columns=filtered_data.columns.tolist()
    titles=filtered_data.values.tolist()
    return render_template('index2.html', tables=columns,titles=titles,currtable='Manual/Automatic Cars Data')
@app.route('/hondaPetrolSales',methods=['GET','POST'])
def hondasales():
    first_data=get_csv_from_s3(S3_BUCKET,'Car_sales_transactions.csv')
    second_data=get_csv_from_s3(S3_BUCKET,'Sales_Status.csv')
    def extract_numeric_mileage(mileage):
        match = re.search(r'^[0-9]+(\.[0-9]+)?', mileage)
        return float(match.group(0)) if match else None
    first_data['NumericMileage'] = first_data['Mileage'].apply(extract_numeric_mileage)
    merged_data = pd.merge(first_data,second_data,on='Sales_ID')
    filtered_data = merged_data[
    (merged_data['Name'].str.startswith('Honda')) &
    (merged_data['Owner'].isin(['First Owner', 'Second Owner'])) &
    (merged_data['Fuel'] == 'Petrol') &
    (merged_data['NumericMileage'] == 17.0) &
    (merged_data['Sold'] == 'Y') &
    (merged_data['Seats'] >= 4)]
    columns=filtered_data.columns.tolist()
    titles=filtered_data.values.tolist()
    return render_template('index2.html', tables=columns,titles=titles,currtable='Honda Sales')
@app.route('/search_car', methods=['GET', 'POST'])
def search_car():
    if request.method == 'POST':
        car_id = request.form.get('car_id')
        return redirect(url_for('show_car_details', car_id=car_id))
    return render_template('search_car.html')
@app.route('/car_details/<car_id>', methods=['GET','POST'])
def show_car_details(car_id):
    car_id = int(car_id)
    car_sales_transactions = get_csv_from_s3(S3_BUCKET,'Car_sales_transactions.csv')
    sales_status = get_csv_from_s3(S3_BUCKET,'Sales_Status.csv')
    city_master = get_csv_from_s3(S3_BUCKET,'City_Master.csv')
    state_master = get_csv_from_s3(S3_BUCKET,'State_Master.csv')
    postal_code=get_csv_from_s3(S3_BUCKET,'Postal_Code_Master.csv')
    state_mapping=get_csv_from_s3(S3_BUCKET,'Region_State_Mapping.csv')
    region_master=get_csv_from_s3(S3_BUCKET,'Region_Master.csv')
    if 'State_Code' not in car_sales_transactions.columns:
        return "<h3 style='margin-left:40%;'>State_Code column is missing in car_sales_transactions</h3>", 404
    if 'State_Code' not in state_master.columns:
        return "<h3 style='margin-left:40%;'>State_Code column is missing in state_master</h3>", 404

    # Perform the joins
    #merged_data = car_sales_transactions.merge(city_master, on='City_Code')\
    #                                       .merge(state_master, on='State_Code')\
    #                                        .merge(sales_status, on='Sales_ID')

    # Filter the data based on the car_id
    merged_data = car_sales_transactions.merge(sales_status, on='Sales_ID')
    car_details = merged_data[merged_data['Sales_ID'] == car_id]
    if car_details.empty:
        return "<h3 style='margin-left:40%;'>Car Not Found With This Id</h3>", 404

    #car_details_dict = car_details.to_dict(orient='records')[0]
    columns=car_details.columns.tolist()
    titles=car_details.values.tolist()
    return render_template('index2.html', tables=columns,titles=titles,currtable='Car Details')
if __name__ == '__main__':
    app.run(debug=True,host='0.0.0.0',port=8081)
