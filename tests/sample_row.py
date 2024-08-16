"""Sample row for helpers test"""
import datetime
from decimal import Decimal

test_row = [[2, datetime.datetime(2022, 11, 3, 14, 20, 52, 186000), 
            datetime.datetime(2022, 11, 3, 14, 20, 52, 186000), 3, 19, 8, 42972, Decimal('3.94'), 2, '2022-11-07', '2022-11-08', 8]]

test_columns = [{'name': 'sales_order_id'}, {'name': 'created_at'}, {'name': 'last_updated'}, {'name': 'design_id'}, {'name': 'staff_id'}, {'name': 'counterparty_id'}, {'name': 'units_sold'}, {'name': 'unit_price'}, {'name': 'currency_id'}, {'name': 'agreed_delivery_date'}, {'name': 'agreed_payment_date'}, {'name': 'agreed_delivery_location_id'}]

resulting_row = [{'sales_order_id': 2, 'created_at': datetime.datetime(2022, 11, 3, 14, 20, 52, 186000), 'last_updated': datetime.datetime(2022, 11, 3, 14, 20, 52, 186000), 'design_id': 3, 'staff_id': 19, 'counterparty_id': 8, 'units_sold': 42972, 'unit_price': Decimal('3.94'), 'currency_id': 2, 'agreed_delivery_date': '2022-11-07', 'agreed_payment_date': '2022-11-08', 'agreed_delivery_location_id': 8}]