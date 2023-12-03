import pandas as pd

client_file = 'dataset/client.csv'
district_file = 'dataset/district.csv'
account_file = 'dataset/account.csv'
disp_file = 'dataset/disp.csv'
card_dev_file = 'dataset/card_dev.csv'
loan_dev_file = 'dataset/loan_dev.csv'
trans_dev_file = 'dataset/trans_dev.csv'

client_df = pd.read_csv(client_file, sep=";")
district_df = pd.read_csv(district_file, sep=";")
account_df = pd.read_csv(account_file, sep=";")
disp_df = pd.read_csv(disp_file, sep=";")
card_dev_df = pd.read_csv(card_dev_file, sep=";")
loan_dev_df = pd.read_csv(loan_dev_file, sep=";")
trans_dev_df = pd.read_csv(trans_dev_file, sep=";", dtype={'bank': str, 'account': str})


# -- client file --
print("Client File")
# check if client_id is unique
client_id_unique = not client_df['client_id'].duplicated().any()
print("Client IDs are unique: ", client_id_unique)
# remove rows with invalid client_id
client_df = client_df.drop_duplicates(subset=['client_id'])

# check gender and age based on birth_number
client_df['Gender'] = client_df['birth_number'].apply(lambda x: 'M' if int(str(x)[2:4]) <= 12 else 'F' if int(str(x)[2:4])-50 <= 12 else 'Unknown')
# parse birth_number to datetime
client_df['birth_number'] = client_df['birth_number'].apply(lambda x: x if int(str(x)[2:4]) <= 12 else x-5000)
client_df['birth_date'] = pd.to_datetime(client_df['birth_number'].apply(lambda x: f"{int(str(x)[:2]) + 1900 if int(str(x)[:2]) > 23 else int(str(x)[:2]) + 2000}-{int(str(x)[2:4])}-{int(str(x)[4:6])}"))
client_df['Age'] = client_df['birth_number'].apply(lambda x: 2023 - (int(str(x)[:2]) + 1900) if int(str(x)[:2]) > 23 else 2023 - (int(str(x)[:2]) + 2000))

count_unknown_gender = client_df[client_df['Gender'] == 'Unknown'].count()['Gender']
print("Number of unknowns in gender: ", count_unknown_gender)

# remove clients with age < 18 ?
count_under_18 = client_df[client_df['Age'] < 18].count()['Age']
print("Number of clients under 18: ", count_under_18)
client_df = client_df.drop(client_df[(client_df['Age'] < 18)].index)

# remove clients with age > 90 ?
count_over_90 = client_df[client_df['Age'] > 90].count()['Age']
print("Number of clients over 90: ", count_over_90)
client_df = client_df.drop(client_df[(client_df['Age'] > 90)].index)

# check if district_id exists in district.csv
# Note: it needs to be 'code ' because of the space in the column name
client_district_exists = client_df['district_id'].isin(district_df['code '])
rows_with_invalid_client_district = client_df[~client_district_exists].count()['district_id']
print("Number of rows with invalid district: ", rows_with_invalid_client_district)

print(round(client_df.describe(), 2))
print(client_df.info())

# -- district file --
print("\nDistrict File")

# check if district_id is unique
district_id_unique = not district_df['code '].duplicated().any()
print("District IDs are unique: ", district_id_unique)
# remove rows with invalid district_id
district_df = district_df.drop_duplicates(subset=['code '])

print(round(district_df.describe(), 2))
print(district_df.info())

# -- account file --
print("\nAccount File")
# check if account_id is unique
account_id_unique = not account_df['account_id'].duplicated().any()
print("Account IDs are unique: ", account_id_unique)
# remove rows with invalid account_id
account_df = account_df.drop_duplicates(subset=['account_id'])

# check if district_id exists in district.csv
# Note: it needs to be 'code ' because of the space in the column name
account_district_exists = account_df['district_id'].isin(district_df['code '])
rows_with_invalid_account_district = account_df[~account_district_exists].count()['district_id']
print("Number of rows with invalid district: ", rows_with_invalid_account_district)

# parse date to datetime
account_df['date'] = pd.to_datetime(account_df['date'].apply(lambda x: f"{int(str(x)[:2]) + 1900 if int(str(x)[:2]) > 23 else int(str(x)[:2]) + 2000}-{int(str(x)[2:4])}-{int(str(x)[4:6])}"))

# check if account creation date is after birth date
merge_disp_client_data = pd.merge(disp_df, client_df, on='client_id')
merge_client_account_data = pd.merge(merge_disp_client_data, account_df, on='account_id')
merge_client_account_data['is_valid_account_creation_date'] = ((merge_client_account_data['date'] - merge_client_account_data['birth_date']).dt.days / 365) >= 18
# count number of invalid account creation dates
count_invalid_account_creation_date = merge_client_account_data[merge_client_account_data['is_valid_account_creation_date'] == False].count()['is_valid_account_creation_date']
print("Number of invalid account creation dates (before birthdate + 18): ", count_invalid_account_creation_date)
# update account_df
# Note: it had to be like this, or else the account_df would not update
account_df = merge_client_account_data[['account_id', 'district_id_y', 'frequency', 'date', 'is_valid_account_creation_date']]
# remove rows with invalid account creation dates
account_df = account_df.drop(account_df[(account_df['is_valid_account_creation_date'] == False)].index)
# remove clients with no account
client_df_updated = merge_client_account_data[['client_id', 'birth_number', 'district_id_x', 'birth_date', 'Age', 'Gender', 'is_valid_account_creation_date']]
client_df_updated = client_df_updated.drop(client_df_updated[(client_df_updated['is_valid_account_creation_date'] == False)].index)
# rename district_id_y to district_id in account_df
account_df = account_df.rename(columns={'district_id_y': 'district_id'})
# rename district_id_x to district_id in client_df
client_df_updated = client_df_updated.rename(columns={'district_id_x': 'district_id'})
# update client_df
client_df.update(client_df_updated)

print(round(account_df.describe(), 2))
print(account_df.info())

# -- disp file --
print("\nDisp File")

# check if disp_id is unique
disp_id_unique = not disp_df['disp_id'].duplicated().any()
print("Disp IDs are unique: ", disp_id_unique)
# remove rows with invalid disp_id
disp_df = disp_df.drop_duplicates(subset=['disp_id'])

# check if account_id exists in account.csv
disp_account_exists = disp_df['account_id'].isin(account_df['account_id'])
rows_with_invalid_disp_account = disp_df[~disp_account_exists].count()['account_id']
print("Number of rows with invalid account: ", rows_with_invalid_disp_account)
# remove rows with invalid account_id
disp_df = disp_df.drop(disp_df[(~disp_account_exists)].index)

# check if client_id exists in client.csv
disp_client_exists = disp_df['client_id'].isin(client_df['client_id'])
rows_with_invalid_disp_client = disp_df[~disp_client_exists].count()['client_id']
print("Number of rows with invalid client: ", rows_with_invalid_disp_client)
# remove rows with invalid client_id
disp_df = disp_df.drop(disp_df[(~disp_client_exists)].index)

print(round(disp_df.describe(), 2))
print(disp_df.info())

# -- card_dev file --
print("\nCard_dev File")

# check if card_id is unique
card_id_unique = not card_dev_df['card_id'].duplicated().any()
print("Card IDs are unique: ", card_id_unique)
# remove rows with invalid card_id
card_dev_df = card_dev_df.drop_duplicates(subset=['card_id'])

# check if disp_id exists in disp.csv
card_dev_disp_exists = card_dev_df['disp_id'].isin(disp_df['disp_id'])
rows_with_invalid_card_dev_disp = card_dev_df[~card_dev_disp_exists].count()['disp_id']
print("Number of rows with invalid disp: ", rows_with_invalid_card_dev_disp)
# remove rows with invalid disp_id
card_dev_df = card_dev_df.drop(card_dev_df[(~card_dev_disp_exists)].index)

# parse issued to datetime
card_dev_df['issued'] = pd.to_datetime(card_dev_df['issued'].apply(lambda x: f"{int(str(x)[:2]) + 1900 if int(str(x)[:2]) > 23 else int(str(x)[:2]) + 2000}-{int(str(x)[2:4])}-{int(str(x)[4:6])}"))

# check if card creation date is after birth date
merge_card_disp_data = pd.merge(card_dev_df, disp_df, on='disp_id')
merge_card_client_data = pd.merge(merge_card_disp_data, client_df, on='client_id')
merge_card_client_data['is_valid_card_creation_date'] = ((merge_card_client_data['issued'] - merge_card_client_data['birth_date']).dt.days / 365) >= 18
# count number of invalid card creation dates
count_invalid_card_creation_date = merge_card_client_data[merge_card_client_data['is_valid_card_creation_date'] == False].count()['is_valid_card_creation_date']
print("Number of invalid card creation dates (issued before birthdate + 18 years): ", count_invalid_card_creation_date)
# remove rows with invalid card creation dates
merge_card_client_data = merge_card_client_data.drop(merge_card_client_data[(merge_card_client_data['is_valid_card_creation_date'] == False)].index)
card_dev_df_updated = merge_card_client_data[['card_id', 'disp_id', 'type_x', 'issued', 'age_at_card_creation', 'is_valid_card_creation_date']]
# rename type_x to type
card_dev_df_updated = card_dev_df_updated.rename(columns={'type_x': 'type'})
# update card_dev_df
card_dev_df.update(card_dev_df_updated)

# check if card creation date is after account creation date
merge_card_account_data = pd.merge(merge_card_disp_data, account_df, on='account_id')
merge_card_account_data['is_valid_card_creation_date'] = merge_card_account_data['issued'] >= merge_card_account_data['date']
# count number of invalid card creation dates
count_invalid_card_creation_date = merge_card_account_data[merge_card_account_data['is_valid_card_creation_date'] == False].count()['is_valid_card_creation_date']
print("Number of invalid card creation dates (issued before account creation): ", count_invalid_card_creation_date)
# remove rows with invalid card creation dates
merge_card_account_data = merge_card_account_data.drop(merge_card_account_data[(merge_card_account_data['is_valid_card_creation_date'] == False)].index)
card_dev_df_updated = merge_card_account_data[['card_id', 'disp_id', 'type_x', 'issued', 'is_valid_card_creation_date']]
# rename type_x to type
card_dev_df_updated = card_dev_df_updated.rename(columns={'type_x': 'type'})
# update card_dev_df
card_dev_df.update(card_dev_df_updated)

print(round(card_dev_df.describe(), 2))
print(card_dev_df.info())

# -- loan_dev file --
print("\nLoan_dev File")

# check if loan_id is unique
loan_id_unique = not loan_dev_df['loan_id'].duplicated().any()
print("Loan IDs are unique: ", loan_id_unique)
# remove rows with invalid loan_id
loan_dev_df = loan_dev_df.drop_duplicates(subset=['loan_id'])

# check if account_id exists in account.csv
loan_dev_account_exists = loan_dev_df['account_id'].isin(account_df['account_id'])
rows_with_invalid_loan_dev_account = loan_dev_df[~loan_dev_account_exists].count()['account_id']
print("Number of rows with invalid account: ", rows_with_invalid_loan_dev_account)
# remove rows with invalid account_id
loan_dev_df = loan_dev_df.drop(loan_dev_df[(~loan_dev_account_exists)].index)

# parse date to datetime
loan_dev_df['date'] = pd.to_datetime(loan_dev_df['date'].apply(lambda x: f"{int(str(x)[:2]) + 1900 if int(str(x)[:2]) > 23 else int(str(x)[:2]) + 2000}-{int(str(x)[2:4])}-{int(str(x)[4:6])}"))

# check if loan date is after account creation date
merge_loan_account_data = pd.merge(loan_dev_df, account_df, on='account_id')
merge_loan_account_data['is_valid_loan_date'] = merge_loan_account_data['date_x'] >= merge_loan_account_data['date_y']
# count number of invalid loan dates
count_invalid_loan_date = merge_loan_account_data[merge_loan_account_data['is_valid_loan_date'] == False].count()['is_valid_loan_date']
print("Number of invalid loan dates (before account creation): ", count_invalid_loan_date)
# remove rows with invalid loan dates
merge_loan_account_data = merge_loan_account_data.drop(merge_loan_account_data[(merge_loan_account_data['is_valid_loan_date'] == False)].index)
loan_dev_df_updated = merge_loan_account_data[['loan_id', 'account_id', 'date_x', 'amount', 'duration', 'payments', 'status', 'is_valid_loan_date']]
# rename date_x to date
loan_dev_df_updated = loan_dev_df_updated.rename(columns={'date_x': 'date'})
# update loan_dev_df
loan_dev_df.update(loan_dev_df_updated)

# check if loan date is after birth date
merge_loan_disp_data = pd.merge(loan_dev_df, disp_df, on='account_id')
merge_loan_client_data = pd.merge(merge_loan_disp_data, client_df, on='client_id')
merge_loan_client_data['is_valid_loan_date'] = ((merge_loan_client_data['date'] - merge_loan_client_data['birth_date']).dt.days / 365) >= 18
# count number of invalid loan dates
count_invalid_loan_date = merge_loan_client_data[merge_loan_client_data['is_valid_loan_date'] == False].count()['is_valid_loan_date']
print("Number of invalid loan dates (before birthdate + 18 years): ", count_invalid_loan_date)
# remove rows with invalid loan dates
merge_loan_client_data = merge_loan_client_data.drop(merge_loan_client_data[(merge_loan_client_data['is_valid_loan_date'] == False)].index)
loan_dev_df_updated = merge_loan_client_data[['loan_id', 'account_id', 'date', 'amount', 'duration', 'payments', 'status', 'is_valid_loan_date']]
# update loan_dev_df
loan_dev_df.update(loan_dev_df_updated)

print(round(loan_dev_df.describe(), 2))
print(loan_dev_df.info())

# -- trans_dev file --
print("\nTrans_dev File")

# check if trans_id is unique
trans_id_unique = not trans_dev_df['trans_id'].duplicated().any()
print("Trans IDs are unique: ", trans_id_unique)
# remove rows with invalid trans_id
trans_dev_df = trans_dev_df.drop_duplicates(subset=['trans_id'])

# check if account_id exists in account.csv
trans_dev_account_exists = trans_dev_df['account_id'].isin(account_df['account_id'])
rows_with_invalid_trans_dev_account = trans_dev_df[~trans_dev_account_exists].count()['account_id']
print("Number of rows with invalid account: ", rows_with_invalid_trans_dev_account)
# remove rows with invalid account_id
trans_dev_df = trans_dev_df.drop(trans_dev_df[(~trans_dev_account_exists)].index)

# parse date to datetime
trans_dev_df['date'] = pd.to_datetime(trans_dev_df['date'].apply(lambda x: f"{int(str(x)[:2]) + 1900 if int(str(x)[:2]) > 23 else int(str(x)[:2]) + 2000}-{int(str(x)[2:4])}-{int(str(x)[4:6])}"))

# check if trans date is after account creation date
merge_trans_account_data = pd.merge(trans_dev_df, account_df, on='account_id')
merge_trans_account_data['is_valid_trans_date'] = merge_trans_account_data['date_x'] >= merge_trans_account_data['date_y']
# count number of invalid trans dates
count_invalid_trans_date = merge_trans_account_data[merge_trans_account_data['is_valid_trans_date'] == False].count()['is_valid_trans_date']
print("Number of invalid trans dates (before account creation): ", count_invalid_trans_date)
# remove rows with invalid trans dates
merge_trans_account_data = merge_trans_account_data.drop(merge_trans_account_data[(merge_trans_account_data['is_valid_trans_date'] == False)].index)
trans_dev_df_updated = merge_trans_account_data[['trans_id', 'account_id', 'date_x', 'type', 'operation', 'amount', 'balance', 'k_symbol', 'bank', 'account']]
# rename date_x to date
trans_dev_df_updated = trans_dev_df_updated.rename(columns={'date_x': 'date'})
# update trans_dev_df
trans_dev_df.update(trans_dev_df_updated)

# check if trans date is after birth date
merge_trans_disp_data = pd.merge(trans_dev_df, disp_df, on='account_id')
merge_trans_client_data = pd.merge(merge_trans_disp_data, client_df, on='client_id')
merge_trans_client_data['is_valid_trans_date'] = ((merge_trans_client_data['date'] - merge_trans_client_data['birth_date']).dt.days / 365) >= 18
# count number of invalid trans dates
count_invalid_trans_date = merge_trans_client_data[merge_trans_client_data['is_valid_trans_date'] == False].count()['is_valid_trans_date']
print("Number of invalid trans dates (before birthdate + 18 years): ", count_invalid_trans_date)
# remove rows with invalid trans dates
merge_trans_client_data = merge_trans_client_data.drop(merge_trans_client_data[(merge_trans_client_data['is_valid_trans_date'] == False)].index)
trans_dev_df_updated = merge_trans_client_data[['trans_id', 'account_id', 'date', 'type_x', 'operation', 'amount', 'balance', 'k_symbol', 'bank', 'account']]
# rename type_x to type
trans_dev_df_updated = trans_dev_df_updated.rename(columns={'type_x': 'type'})
# update trans_dev_df
trans_dev_df.update(trans_dev_df_updated)


print(round(trans_dev_df.describe(), 2))
print(trans_dev_df.info())