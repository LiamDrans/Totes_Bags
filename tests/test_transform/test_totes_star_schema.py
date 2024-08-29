import unittest

import boto3
import pandas as pd
from moto import mock_aws

from src.transform.app.totes_star_schema import pull_latest_json_from_data_bucket, format_list_to_dict_of_dataframes, \
    dim_design, fact_payment, fact_sales_order, fact_purchase_order, dim_currency, dim_location, dim_transaction, \
    dim_payment_type, dim_counterparty, dim_staff, transform_all_tables


@mock_aws
def test_pull_latest_json_returns_appropriate_json():
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='totes-data-1234124', CreateBucketConfiguration={
        'LocationConstraint': 'eu-west-2'})
    s3.put_object(Bucket='totes-data-1234124', Key='latest_db_totes.json',
                  Body='[{"test": [{"id": 1, "num": 1, "last_updated": "Thu, 03 Nov 2022 14:20:49 GMT"}]}, {"test2": [{"id": 1, "num": 2, "last_updated": "Thu, 03 Nov 2022 14:20:49 GMT"}]}]')
    (data_list, table_names) = pull_latest_json_from_data_bucket()
    assert data_list == [{'test': [{'id': 1, 'num': 1, 'last_updated': 'Thu, 03 Nov 2022 14:20:49 GMT'}]},
                         {'test2': [{'id': 1, 'num': 2, 'last_updated': 'Thu, 03 Nov 2022 14:20:49 GMT'}]}]
    assert table_names == ['test', 'test2']


def test_format_list_to_dict_of_dataframes_returns_appropriate_dict():
    data_list = [{'test': [{'id': 1, 'num': 1, 'last_updated': 'Thu, 03 Nov 2022 14:20:49 GMT'}]},
                 {'test2': [{'id': 1, 'num': 2, 'last_updated': 'Thu, 03 Nov 2022 14:20:49 GMT'}]}]
    table_names = ['test', 'test2']
    dict_of_dataframes = format_list_to_dict_of_dataframes(data_list, table_names)
    assert dict_of_dataframes['test'].columns.tolist() == ['id', 'num', 'last_updated']
    assert dict_of_dataframes['test2'].columns.tolist() == ['id', 'num', 'last_updated']


class TestTableDimensions(unittest.TestCase):
    def setUp(self):
        self.data = [{'address': [{'address_id': 1, 'address_line_1': '6826 Herzog Via', 'address_line_2': None, 'district': 'Avon', 'city': 'New Patienceburgh', 'postal_code': '28441', 'country': 'Turkey', 'phone': '1803 637401', 'created_at': 'Thu, 03 Nov 2022 14:20:49 GMT', 'last_updated': 'Thu, 03 Nov 2022 14:20:49 GMT'}]},
                    {'staff': [{'created_at': 'Thu, 03 Nov 2022 14:20:51 GMT', 'department_id': 2, 'email_address': 'jeremie.franey@terrifictotes.com', 'first_name': 'Jeremie', 'last_name': 'Franey', 'last_updated': 'Thu, 03 Nov 2022 14:20:51 GMT', 'staff_id': 1}]},
                    {'payment': [{'payment_id': 2, 'created_at': 'Thu, 03 Nov 2022 14:20:52 GMT', 'last_updated': 'Thu, 03 Nov 2022 14:20:52 GMT', 'transaction_id': 2, 'counterparty_id': 15, 'payment_amount': 552548.62, 'currency_id': 2, 'payment_type_id': 3, 'paid': False, 'payment_date': '2022-11-04', 'company_ac_number': 67305075, 'counterparty_ac_number': 31622269}]},
                    {'department': [{'created_at': 'Thu, 03 Nov 2022 14:20:49 GMT', 'department_id': 1, 'department_name': 'Sales', 'last_updated': 'Thu, 03 Nov 2022 14:20:49 GMT', 'location': 'Manchester', 'manager': 'Richard Roma'}]},
                    {'transaction': [{'created_at': 'Thu, 03 Nov 2022 14:20:52 GMT', 'last_updated': 'Thu, 03 Nov 2022 14:20:52 GMT', 'purchase_order_id': 2, 'sales_order_id': None, 'transaction_id': 1, 'transaction_type': 'PURCHASE'}]},
                    {'currency': [{'currency_id': 1, 'currency_code': 'GBP', 'created_at': 'Thu, 03 Nov 2022 14:20:49 GMT', 'last_updated': 'Thu, 03 Nov 2022 14:20:49 GMT'}, {'currency_id': 2, 'currency_code': 'USD', 'created_at': 'Thu, 03 Nov 2022 14:20:49 GMT', 'last_updated': 'Thu, 03 Nov 2022 14:20:49 GMT'}, {'currency_id': 3, 'currency_code': 'EUR', 'created_at': 'Thu, 03 Nov 2022 14:20:49 GMT', 'last_updated': 'Thu, 03 Nov 2022 14:20:49 GMT'}]},
                    {'payment_type': [{'payment_type_id': 1, 'payment_type_name': 'SALES_RECEIPT', 'created_at': 'Thu, 03 Nov 2022 14:20:49 GMT', 'last_updated': 'Thu, 03 Nov 2022 14:20:49 GMT'}, {'payment_type_id': 2, 'payment_type_name': 'SALES_REFUND', 'created_at': 'Thu, 03 Nov 2022 14:20:49 GMT', 'last_updated': 'Thu, 03 Nov 2022 14:20:49 GMT'}, {'payment_type_id': 3, 'payment_type_name': 'PURCHASE_PAYMENT', 'created_at': 'Thu, 03 Nov 2022 14:20:49 GMT', 'last_updated': 'Thu, 03 Nov 2022 14:20:49 GMT'}, {'payment_type_id': 4, 'payment_type_name': 'PURCHASE_REFUND', 'created_at': 'Thu, 03 Nov 2022 14:20:49 GMT', 'last_updated': 'Thu, 03 Nov 2022 14:20:49 GMT'}]},
                    {'sales_order': [{'sales_order_id': 2, 'created_at': 'Thu, 03 Nov 2022 14:20:52 GMT', 'last_updated': 'Thu, 03 Nov 2022 14:20:52 GMT', 'design_id': 3, 'staff_id': 19, 'counterparty_id': 8, 'units_sold': 42972, 'unit_price': 3.94, 'currency_id': 2, 'agreed_delivery_date': '2022-11-07', 'agreed_payment_date': '2022-11-08', 'agreed_delivery_location_id': 8}]},
                    {'counterparty': [{'counterparty_id': 1, 'counterparty_legal_name': 'Fahey and Sons', 'legal_address_id': 15, 'commercial_contact': 'Micheal Toy', 'delivery_contact': 'Mrs. Lucy Runolfsdottir', 'created_at': 'Thu, 03 Nov 2022 14:20:51 GMT', 'last_updated': 'Thu, 03 Nov 2022 14:20:51 GMT'}]},
                    {'purchase_order': [{'purchase_order_id': 1, 'created_at': 'Thu, 03 Nov 2022 14:20:52 GMT', 'last_updated': 'Thu, 03 Nov 2022 14:20:52 GMT', 'staff_id': 12, 'counterparty_id': 11, 'item_code': 'ZDOI5EA', 'item_quantity': 371, 'item_unit_price': 361.39, 'currency_id': 2, 'agreed_delivery_date': '2022-11-09', 'agreed_payment_date': '2022-11-07', 'agreed_delivery_location_id': 6}]},
                    {'design': [{'created_at': 'Thu, 03 Nov 2022 14:20:49 GMT', 'design_id': 8, 'design_name': 'Wooden', 'file_location': '/usr', 'file_name': 'wooden-20220717-npgz.json', 'last_updated': 'Thu, 03 Nov 2022 14:20:49 GMT'}]}
                    ]

    def test_transform_all_tables_always_runs_dim_dates(self):
        df = pd.DataFrame()
        df = transform_all_tables(df)
        assert df['dim_date'].columns.tolist() == ['date_id', 'year', 'month', 'day', 'day_of_week', 'day_name', 'month_name', 'quarter']

    def test_dim_design_returns_appropriate_dict(self):
        df = pd.DataFrame(self.data[10]['design'])
        df = dim_design(df)
        assert df.columns.tolist() == ['design_id', 'design_name', 'file_location', 'file_name']

    def test_fact_payment_returns_appropriate_dict(self):
        df = pd.DataFrame(self.data[2]['payment'])
        df = fact_payment(df)
        assert df.columns.tolist() == ['payment_record_id', 'payment_id', 'created_date', 'created_time',
                                       'last_updated_date', 'last_updated_time', 'transaction_id', 'counterparty_id',
                                       'payment_amount', 'currency_id', 'payment_type_id', 'paid', 'payment_date']

    def test_fact_sales_order_returns_appropriate_dict(self):
        df = pd.DataFrame(self.data[7]['sales_order'])
        df = fact_sales_order(df)
        assert df.columns.tolist() == ['sales_record_id',
                                       'sales_order_id',
                                       'created_date',
                                       'created_time',
                                       'last_updated_date',
                                       'last_updated_time',
                                       'sales_staff_id',
                                       'counterparty_id',
                                       'units_sold',
                                       'unit_price',
                                       'currency_id',
                                       'design_id',
                                       'agreed_payment_date',
                                       'agreed_delivery_date',
                                       'agreed_delivery_location_id']

    def test_fact_purchase_order_returns_appropriate_dict(self):
        df = pd.DataFrame(self.data[9]['purchase_order'])
        df = fact_purchase_order(df)
        assert df.columns.tolist() == ['purchase_record_id',
                                       'purchase_order_id',
                                       'created_date',
                                       'created_time',
                                       'last_updated_date',
                                       'last_updated_time',
                                       'staff_id',
                                       'counterparty_id',
                                       'item_code',
                                       'item_quantity',
                                       'item_unit_price',
                                       'currency_id',
                                       'agreed_delivery_date',
                                       'agreed_payment_date',
                                       'agreed_delivery_location_id']

    def test_dim_currency_returns_appropriate_dict(self):
        df = pd.DataFrame(self.data[5]['currency'])
        df = dim_currency(df)
        assert df.columns.tolist() == ['currency_id', 'currency_code', 'currency_name']

    def test_dim_location_returns_appropriate_dict(self):
        df = pd.DataFrame(self.data[0]['address'])
        df = dim_location(df)
        assert df.columns.tolist() == ['location_id', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone']

    def test_dim_transaction_returns_appropriate_dict(self):
        df = pd.DataFrame(self.data[4]['transaction'])
        df = dim_transaction(df)
        assert df.columns.tolist() == ['purchase_order_id', 'sales_order_id','transaction_id', 'transaction_type']

    def test_dim_payment_type_returns_appropriate_dict(self):
        df = pd.DataFrame(self.data[6]['payment_type'])
        df = dim_payment_type(df)
        assert df.columns.tolist() == ['payment_type_id', 'payment_type_name']

    def test_dim_counterparty_returns_appropriate_dict(self):
        df_counterparty = pd.DataFrame(self.data[8]['counterparty'])
        df_address = pd.DataFrame(self.data[0]['address'])
        df = dim_counterparty(df_counterparty, df_address)
        assert df.columns.tolist() == ['counterparty_id', 'counterparty_legal_name', 'counterparty_legal_address_line_1', 'counterparty_legal_address_line_2', 'district', 'counterparty_legal_city', 'counterparty_legal_postal_code', 'counterparty_legal_country', 'counterparty_legal_phone_number']

    def test_dim_staff_department_returns_appropriate_dict(self):
        df_staff = pd.DataFrame(self.data[1]['staff'])
        df_department = pd.DataFrame(self.data[3]['department'])
        df = dim_staff(df_staff, df_department)
        assert df.columns.tolist() == ['staff_id', 'first_name', 'last_name', 'department_name', 'location', 'email_address']

if __name__ == '__main__':
    unittest.main()
