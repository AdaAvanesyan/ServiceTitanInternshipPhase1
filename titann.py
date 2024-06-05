# import pandas as pd

# class DataExtractor:
#     def __init__(self, invoices_file, expired_invoices_file):
#         self.invoices_file = invoices_file
#         self.expired_invoices_file = expired_invoices_file
#         self.expired_invoice_ids = self.load_expired_invoices()

#     def load_expired_invoices(self):
#         with open(self.expired_invoices_file, 'r') as file:
#             expired_ids = file.read().strip().split(',')
#         return set(map(int, expired_ids))

#     def load_data(self):
#         invoices = pd.read_pickle(self.invoices_file)
#         return invoices

#     def inspect_data(self, invoices):
#         if invoices:
#             first_invoice = invoices[0]
#             print(f"Keys in the first invoice: {first_invoice.keys()}")
#             if first_invoice['items']:
#                 first_item = first_invoice['items'][0]
#                 print(f"Keys in the first item: {first_item.keys()}")
#                 print(f"Sample item: {first_item}")
#             else:
#                 print("No items in the first invoice.")
#         else:
#             print("No invoices found.")

# data_extractor = DataExtractor('C:/Users/Adrine Avanesyan/Downloads/invoices_new.pkl', 'C:/Users/Adrine Avanesyan/Downloads/expired_invoices.txt')
# invoices = data_extractor.load_data()
# data_extractor.inspect_data(invoices)

import pandas as pd

class DataExtractor:
    def __init__(self, invoices_file, expired_invoices_file):
        self.invoices_file = invoices_file
        self.expired_invoices_file = expired_invoices_file
        self.expired_invoice_ids = self.load_expired_invoices()

    def load_expired_invoices(self):
        with open(self.expired_invoices_file, 'r') as file:
            expired_ids = file.read().strip().split(',')
        return set(map(int, expired_ids))

    def load_data(self):
        invoices = pd.read_pickle(self.invoices_file)
        return invoices

    def safe_int_conversion(self, value):
        try:
            return int(value)
        except (ValueError, TypeError):
            return 0

    def transform_data(self, invoices):
        type_conversion = {0: 'Material', 1: 'Equipment', 2: 'Service', 3: 'Other'}
        data = []

        
        for invoice in invoices:
            if 'id' not in invoice or 'created_on' not in invoice or 'items' not in invoice:
                print(f"Missing keys in invoice: {invoice.keys()}")
                continue

            invoice_id = self.safe_int_conversion(invoice['id'])
            created_on = pd.to_datetime(invoice['created_on'], errors='coerce')

            if created_on is pd.NaT:
                print(f"Invalid date in invoice {invoice_id}")
                continue

            if not isinstance(invoice['items'], list):
                print(f"Invalid items in invoice {invoice_id}")
                continue

            invoice_total = sum(
                self.safe_int_conversion(item.get('item', {}).get('unit_price')) * self.safe_int_conversion(item.get('quantity'))
                for item in invoice['items']
            )

            for item in invoice['items']:
                item_info = item.get('item', {})
                if not item_info:
                    print(f"Missing item info in invoice {invoice_id}")
                    continue

                invoiceitem_id = self.safe_int_conversion(item_info.get('id'))
                invoiceitem_name = item_info.get('name', '')
                item_type = type_conversion.get(self.safe_int_conversion(item_info.get('type')), 'Other')
                unit_price = self.safe_int_conversion(item_info.get('unit_price'))
                quantity = self.safe_int_conversion(item.get('quantity'))
                total_price = unit_price * quantity
                percentage_in_invoice = total_price / invoice_total if invoice_total else 0.0
                is_expired = invoice_id in self.expired_invoice_ids

                data.append([
                    invoice_id, created_on, invoiceitem_id, invoiceitem_name, item_type,
                    unit_price, total_price, percentage_in_invoice, is_expired
                ])

        columns = [
            'invoice_id', 'created_on', 'invoiceitem_id', 'invoiceitem_name', 'type',
            'unit_price', 'total_price', 'percentage_in_invoice', 'is_expired'
        ]
        df = pd.DataFrame(data, columns=columns)

       
        df['invoice_id'] = df['invoice_id'].astype(int)
        df['created_on'] = pd.to_datetime(df['created_on'], errors='coerce')
        df['invoiceitem_id'] = df['invoiceitem_id'].astype(int)
        df['invoiceitem_name'] = df['invoiceitem_name'].astype(str)
        df['type'] = df['type'].astype(str)
        df['unit_price'] = df['unit_price'].astype(int)
        df['total_price'] = df['total_price'].astype(int)
        df['percentage_in_invoice'] = df['percentage_in_invoice'].astype(float)
        df['is_expired'] = df['is_expired'].astype(bool)
        df = df.sort_values(by=['invoice_id', 'invoiceitem_id']).reset_index(drop=True)

        return df

    def extract_and_transform(self):
        invoices = self.load_data()
        transformed_data = self.transform_data(invoices)
        return transformed_data


data_extractor = DataExtractor('C:/Users/Adrine Avanesyan/Downloads/invoices_new.pkl', 'C:/Users/Adrine Avanesyan/Downloads/expired_invoices.txt')
transformed_df = data_extractor.extract_and_transform()
print(transformed_df.head())


