import random
import json
from datetime import datetime, timedelta
from faker import Faker
import uuid
from typing import Dict, List, Tuple
import os
import pandas as pd
import sys
import string
import re

class RetailDataGenerator:
    def __init__(self, seed=42, add_noise=True):
        """Initialize the retail data generator with configurable parameters."""
        self.fake = Faker()
        Faker.seed(seed)
        random.seed(seed)
        
        # Configuration
        self.num_products = 12000
        self.num_customers = 55000
        self.target_monthly_transactions = 120000
        self.daily_transactions = self.target_monthly_transactions // 30
        self.add_noise = add_noise
        
        # Enhanced data quality configuration with realistic enterprise issues
        self.noise_config = {
            # Missing Data Issues (30% overall missing rate)
            'missing_customer_email': 0.35,      # 35% missing emails
            'missing_customer_phone': 0.40,      # 40% missing phone numbers
            'missing_customer_address': 0.25,    # 25% missing addresses
            'missing_product_description': 0.30, # 30% missing descriptions
            'missing_product_sku': 0.15,         # 15% missing SKUs
            'missing_cashier_id': 0.20,          # 20% missing cashier IDs
            'missing_payment_method': 0.10,      # 10% missing payment method
            'missing_transaction_time': 0.05,    # 5% missing precise time
            
            # Duplicate Issues (30% overall duplicate rate)
            'duplicate_customers': 0.30,         # 30% duplicate customers
            'duplicate_transactions': 0.15,      # 15% duplicate transactions
            'duplicate_products': 0.25,          # 25% duplicate products
            'duplicate_stores': 0.20,            # 20% duplicate stores
            
            # Data Quality Issues
            'invalid_email_format': 0.12,        # 12% invalid emails
            'inconsistent_phone_format': 0.25,   # 25% inconsistent phone formats
            'price_inconsistency': 0.08,         # 8% price errors
            'negative_quantities': 0.03,         # 3% negative quantities
            'extreme_prices': 0.05,              # 5% unrealistic prices
            'future_dates': 0.02,                # 2% future dates
            'encoding_issues': 0.08,             # 8% special character issues
            'data_type_inconsistency': 0.15,     # 15% wrong data types
            'reference_integrity_errors': 0.10,  # 10% invalid references
            
            # Timestamp Issues
            'timestamp_delays': 0.25,            # 25% delayed timestamps
            'batch_processing_delays': 0.10,     # 10% batch delays
            'timezone_inconsistencies': 0.08,    # 8% timezone issues
            'system_clock_drift': 0.05,          # 5% clock drift
        }
        
        # Product categories and their typical price ranges
        self.product_categories = {
            'Electronics': (50, 2000),
            'Clothing': (15, 300),
            'Home & Garden': (10, 500),
            'Books': (8, 60),
            'Sports & Outdoors': (20, 800),
            'Beauty & Personal Care': (5, 150),
            'Food & Beverages': (2, 50),
            'Toys & Games': (10, 200),
            'Automotive': (15, 1500),
            'Health & Wellness': (10, 300),
            'Office Supplies': (3, 200),
            'Pet Supplies': (5, 100)
        }
        
        # Payment methods with realistic distribution
        self.payment_methods = {
            'Credit Card': 0.45,
            'Debit Card': 0.25,
            'Cash': 0.15,
            'Digital Wallet': 0.10,
            'Gift Card': 0.05
        }
        
        # Store data
        self.stores = []
        self.products = []
        self.customers = []
        
        # Track duplicates for reference
        self.duplicate_customers = []
        self.duplicate_products = []
        self.duplicate_transactions = []
        
        # Load or generate master data
        self._load_or_generate_master_data()
    
    def _introduce_encoding_issues(self, text):
        """Introduce realistic encoding and special character issues."""
        if not text or not self.add_noise or random.random() > self.noise_config['encoding_issues']:
            return text
        
        issues = [
            lambda s: s.replace('a', 'ä').replace('o', 'ö').replace('u', 'ü'),  # Umlaut issues
            lambda s: s.replace(' ', '\u00A0'),  # Non-breaking spaces
            lambda s: s + '\u200B',  # Zero-width space
            lambda s: s.replace("'", "'"),  # Smart quotes
            lambda s: s.replace('"', '"'),  # Smart quotes
            lambda s: s.encode('latin1', errors='ignore').decode('latin1', errors='ignore'),  # Encoding conversion
            lambda s: s + '�',  # Replacement character
        ]
        
        return random.choice(issues)(text)
    
    # def _introduce_data_type_inconsistency(self, value, target_type='string'):
    #     """Convert values to inconsistent data types."""
    #     if not self.add_noise or random.random() > self.noise_config['data_type_inconsistency']:
    #         return value
        
    #     if target_type == 'string' and isinstance(value, (int, float)):
    #         return str(value)
    #     elif target_type == 'number' and isinstance(value, str):
    #         try:
    #             return float(value) if '.' in value else int(value)
    #         except:
    #             return value
    #     elif isinstance(value, bool):
    #         return random.choice(['true', 'false', '1', '0', 'True', 'False'])
        
    #     return value
    
    def _introduce_timestamp_issues(self, base_datetime):
        """Introduce various timestamp-related issues."""
        if not self.add_noise:
            return base_datetime
        
        modified_datetime = base_datetime
        
        # Timestamp delays (batch processing, system delays)
        if random.random() < self.noise_config['timestamp_delays']:
            delay_minutes = random.choices(
                [5, 15, 30, 60, 120, 240],  # Delay ranges
                weights=[30, 25, 20, 15, 7, 3]  # More common for shorter delays
            )[0]
            modified_datetime = base_datetime + timedelta(minutes=delay_minutes)
        
        # Batch processing delays (transactions recorded in batches)
        if random.random() < self.noise_config['batch_processing_delays']:
            # Round to nearest hour + random batch delay
            modified_datetime = modified_datetime.replace(minute=0, second=0)
            batch_delay = random.randint(1, 6) * 60  # 1-6 hours
            modified_datetime += timedelta(minutes=batch_delay)
        
        # System clock drift
        if random.random() < self.noise_config['system_clock_drift']:
            drift_seconds = random.randint(-300, 300)  # ±5 minutes drift
            modified_datetime += timedelta(seconds=drift_seconds)
        
        # Future timestamps (system clock issues)
        if random.random() < self.noise_config['future_dates']:
            future_days = random.randint(1, 30)
            modified_datetime = base_datetime + timedelta(days=future_days)
        
        # Timezone inconsistencies (recorded in different timezones)
        if random.random() < self.noise_config['timezone_inconsistencies']:
            tz_offset = random.choice([-8, -5, -3, 0, 3, 8])  # Different timezone offsets
            modified_datetime += timedelta(hours=tz_offset)
        
        return modified_datetime
    
    def _load_or_generate_master_data(self, output_dir='/opt/airflow/master_data'):
        """Load existing master data or generate new if it doesn't exist."""
        stores_file = f'{output_dir}/stores.parquet'
        products_file = f'{output_dir}/products.parquet'
        customers_file = f'{output_dir}/customers.parquet'
        
        # Check if all master data files exist
        if (os.path.exists(stores_file) and 
            os.path.exists(products_file) and 
            os.path.exists(customers_file)):
            
            print("Loading existing master data...")
            try:
                # Load existing data
                stores_df = pd.read_parquet(stores_file)
                products_df = pd.read_parquet(products_file)
                customers_df = pd.read_parquet(customers_file)
                
                # Convert back to dictionaries
                self.stores = stores_df.to_dict('records')
                self.products = products_df.to_dict('records')
                self.customers = customers_df.to_dict('records')
                
                print(f"Loaded {len(self.stores)} stores, {len(self.products)} products, {len(self.customers)} customers")
                return
                
            except Exception as e:
                print(f"Error loading master data: {e}")
                print("Generating new master data...")
        
        # Generate new master data if loading failed or files don't exist
        print("Generating new master data...")
        self._generate_stores()
        self._generate_products()
        self._generate_customers()
        
        # Save the newly generated data
        self.save_master_data(output_dir)
    
    def _generate_stores(self):
        """Generate store location data with duplicates and missing information."""
        store_types = ['Flagship', 'Mall', 'Outlet', 'Express', 'Online']
        base_stores = []
        
        for i in range(25):  # 25 base stores
            store = {
                'store_id': f'ST{i+1:03d}',
                'store_name': f'{self.fake.company()} {random.choice(store_types)}',
                'address': self.fake.address(),
                'city': self.fake.city(),
                'state': self.fake.state(),
                'zip_code': self.fake.zipcode(),
                'phone': self.fake.phone_number(),
                'manager': self.fake.name(),
                'store_type': random.choice(store_types),
                'opening_date': self.fake.date_between(start_date='-5y', end_date='today')
            }
            
            # Apply data quality issues
            if self.add_noise:
                # Missing information
                if random.random() < 0.25:
                    store['phone'] = None
                if random.random() < 0.15:
                    store['manager'] = None
                if random.random() < 0.10:
                    store['address'] = None
                
                # Encoding issues
                store['store_name'] = self._introduce_encoding_issues(store['store_name'])
                store['city'] = self._introduce_encoding_issues(store['city'])
                
                # Data type inconsistencies
                # store['zip_code'] = self._introduce_data_type_inconsistency(store['zip_code'], 'string')
            
            base_stores.append(store)
        
        self.stores = base_stores.copy()
        
        # Add duplicate stores with variations
        if self.add_noise:
            num_duplicates = int(len(base_stores) * self.noise_config['duplicate_stores'])
            for _ in range(num_duplicates):
                original = random.choice(base_stores)
                duplicate = original.copy()
                
                # Create variations
                duplicate['store_id'] = f'ST{len(self.stores)+1:03d}'
                duplicate['store_name'] = duplicate['store_name'] + ' Branch'
                
                # Minor address variations
                if duplicate['address']:
                    duplicate['address'] = duplicate['address'].replace('St', 'Street')
                
                self.stores.append(duplicate)
    
    def _generate_products(self):
        """Generate product catalog with extensive data quality issues."""
        brands = [self.fake.company() for _ in range(200)]
        generated_skus = set()
        base_products = []
        
        for i in range(self.num_products):
            category = random.choice(list(self.product_categories.keys()))
            price_range = self.product_categories[category]
            base_price = random.uniform(price_range[0], price_range[1])
            
            # Generate SKU with potential issues
            sku = f'SKU{random.randint(100000, 999999)}'
            if self.add_noise and random.random() < self.noise_config['missing_product_sku']:
                sku = None
            elif sku in generated_skus and random.random() < 0.15:
                sku = f'{sku}-{random.randint(1,99)}'
            
            if sku:
                generated_skus.add(sku)
            
            # Generate description with missing data
            description = self.fake.text(max_nb_chars=200)
            if self.add_noise and random.random() < self.noise_config['missing_product_description']:
                description = None
            
            # Price with quality issues
            cost = round(base_price * random.uniform(0.4, 0.7), 2)
            if self.add_noise:
                if random.random() < self.noise_config['price_inconsistency']:
                    cost = round(base_price * random.uniform(1.1, 1.5), 2)  # Cost > Price
                
                if random.random() < self.noise_config['extreme_prices']:
                    base_price = random.choice([
                        round(random.uniform(0.01, 0.1), 2),  # Too cheap
                        round(random.uniform(50000, 100000), 2)  # Too expensive
                    ])
            
            # Weight and dimensions with errors
            weight = round(random.uniform(0.1, 50), 2)
            dimensions = f'{random.randint(1,50)}x{random.randint(1,50)}x{random.randint(1,50)}'
            
            if self.add_noise and random.random() < 0.15:
                weight = round(random.uniform(0.001, 0.01), 3) if random.choice([True, False]) else round(random.uniform(500, 1000), 2)
            
            product = {
                'product_id': f'PRD{i+1:06d}',
                'product_name': self._generate_product_name(category),
                'category': category,
                'subcategory': self._generate_subcategory(category),
                'brand': random.choice(brands),
                'price': base_price,
                'cost': cost,
                'sku': sku,
                'description': description,
                'weight': weight,
                'dimensions': dimensions,
                'stock_quantity': random.randint(-10, 1000),  # Allow negative stock
                'supplier': self.fake.company(),
                'launch_date': self.fake.date_between(start_date='-2y', end_date='today')
            }
            
            # Apply encoding issues
            if self.add_noise:
                product['product_name'] = self._introduce_encoding_issues(product['product_name'])
                product['brand'] = self._introduce_encoding_issues(product['brand'])
                if product['description']:
                    product['description'] = self._introduce_encoding_issues(product['description'])
                
                # Data type inconsistencies
                # product['stock_quantity'] = self._introduce_data_type_inconsistency(product['stock_quantity'])
            
            base_products.append(product)
        
        self.products = base_products.copy()
        
        # Add duplicate products with variations
        if self.add_noise:
            num_duplicates = int(len(base_products) * self.noise_config['duplicate_products'])
            for _ in range(num_duplicates):
                original = random.choice(base_products)
                duplicate = original.copy()
                
                # Create variations for duplicates
                duplicate['product_id'] = f'PRD{len(self.products)+1:06d}'
                duplicate['sku'] = f"{duplicate['sku']}-DUP" if duplicate['sku'] else None
                
                # Slight price variations
                if isinstance(duplicate['price'], (int, float)):
                    duplicate['price'] = round(duplicate['price'] * random.uniform(0.95, 1.05), 2)
                
                # Different descriptions
                if duplicate['description']:
                    duplicate['description'] = duplicate['description'][:100] + "..."
                
                self.products.append(duplicate)
                self.duplicate_products.append((original['product_id'], duplicate['product_id']))
    
    def _generate_product_name(self, category):
        """Generate realistic product names based on category."""
        adjectives = ['Premium', 'Deluxe', 'Classic', 'Modern', 'Vintage', 'Professional', 'Eco-Friendly']
        
        category_items = {
            'Electronics': ['Smartphone', 'Laptop', 'Tablet', 'Headphones', 'Speaker', 'Camera', 'Monitor'],
            'Clothing': ['T-Shirt', 'Jeans', 'Dress', 'Jacket', 'Sweater', 'Shoes', 'Hat'],
            'Home & Garden': ['Lamp', 'Cushion', 'Vase', 'Plant Pot', 'Tool Set', 'Furniture'],
            'Books': ['Novel', 'Cookbook', 'Biography', 'Guide', 'Textbook', 'Journal'],
            'Sports & Outdoors': ['Running Shoes', 'Backpack', 'Tent', 'Bike', 'Fitness Tracker'],
            'Beauty & Personal Care': ['Shampoo', 'Moisturizer', 'Perfume', 'Makeup Kit', 'Soap'],
            'Food & Beverages': ['Organic Coffee', 'Snack Pack', 'Protein Bar', 'Tea Set', 'Spices'],
            'Toys & Games': ['Board Game', 'Action Figure', 'Puzzle', 'Building Blocks', 'Doll'],
            'Automotive': ['Car Accessories', 'Motor Oil', 'Tire', 'GPS Device', 'Car Charger'],
            'Health & Wellness': ['Vitamins', 'Supplements', 'First Aid Kit', 'Thermometer'],
            'Office Supplies': ['Notebook', 'Pen Set', 'Calculator', 'Stapler', 'Folder'],
            'Pet Supplies': ['Dog Food', 'Cat Toy', 'Pet Bed', 'Leash', 'Pet Carrier']
        }
        
        items = category_items.get(category, ['Product'])
        return f'{random.choice(adjectives)} {random.choice(items)}'
    
    def _generate_subcategory(self, category):
        """Generate subcategories for each main category."""
        subcategories = {
            'Electronics': ['Mobile Phones', 'Computers', 'Audio', 'Gaming', 'Accessories'],
            'Clothing': ['Men\'s Wear', 'Women\'s Wear', 'Children\'s Wear', 'Footwear', 'Accessories'],
            'Home & Garden': ['Furniture', 'Decor', 'Kitchen', 'Garden', 'Storage'],
            'Books': ['Fiction', 'Non-Fiction', 'Educational', 'Children\'s Books', 'Reference'],
            'Sports & Outdoors': ['Fitness', 'Outdoor Recreation', 'Team Sports', 'Water Sports'],
            'Beauty & Personal Care': ['Skincare', 'Haircare', 'Makeup', 'Fragrance', 'Personal Hygiene'],
            'Food & Beverages': ['Beverages', 'Snacks', 'Organic', 'International', 'Gourmet'],
            'Toys & Games': ['Educational', 'Action Figures', 'Board Games', 'Electronic Toys'],
            'Automotive': ['Parts', 'Accessories', 'Maintenance', 'Electronics', 'Tools'],
            'Health & Wellness': ['Supplements', 'Medical Devices', 'Fitness', 'Personal Care'],
            'Office Supplies': ['Stationery', 'Technology', 'Furniture', 'Organization', 'Art Supplies'],
            'Pet Supplies': ['Food', 'Toys', 'Accessories', 'Health', 'Grooming']
        }
        return random.choice(subcategories.get(category, ['General']))
    
    def _generate_customers(self):
        """Generate customer database with extensive data quality issues."""
        generated_emails = set()
        generated_phones = set()
        base_customers = []
        
        for i in range(self.num_customers):
            registration_date = self.fake.date_between(start_date='-3y', end_date='today')
            
            # Generate base customer
            first_name = self.fake.first_name()
            last_name = self.fake.last_name()
            email = self.fake.email()
            phone = self.fake.phone_number()
            
            # Apply extensive data quality issues
            if self.add_noise:
                # Missing data (30% overall missing rate)
                if random.random() < self.noise_config['missing_customer_phone']:
                    phone = None
                if random.random() < self.noise_config['missing_customer_email']:
                    email = None
                
                # Invalid email formats
                if email and random.random() < self.noise_config['invalid_email_format']:
                    email_issues = [
                        lambda e: e.replace('@', '@@'),
                        lambda e: e.replace('.', '..'),
                        lambda e: e + '.invalid',
                        lambda e: e.replace('@', ' at '),
                        lambda e: 'invalid-' + e,
                        lambda e: e[:-4] if e.endswith('.com') else e,  # Remove .com
                    ]
                    email = random.choice(email_issues)(email)
                
                # Inconsistent phone formats
                if phone and random.random() < self.noise_config['inconsistent_phone_format']:
                    phone_formats = [
                        lambda p: re.sub(r'[^\d]', '', p)[:10],  # Numbers only
                        lambda p: f"+1-{p}",  # Add country code
                        lambda p: p.replace('-', '.').replace('(', '').replace(')', ''),
                        lambda p: p + 'x123',  # Add extension
                        lambda p: p[:3] + p[6:] if len(p) > 6 else p,  # Remove area code
                    ]
                    phone = random.choice(phone_formats)(phone)
                
                # Encoding issues in names
                first_name = self._introduce_encoding_issues(first_name)
                last_name = self._introduce_encoding_issues(last_name)
                
                # Data type inconsistencies
                # first_name = self._introduce_data_type_inconsistency(first_name)
                # last_name = self._introduce_data_type_inconsistency(last_name)
            
            # Address components with missing data
            address = self.fake.address()
            city = self.fake.city()
            state = self.fake.state()
            zip_code = self.fake.zipcode()
            
            if self.add_noise and random.random() < self.noise_config['missing_customer_address']:
                missing_components = random.choices(
                    ['address', 'city', 'state', 'zip', 'multiple'],
                    weights=[30, 20, 20, 20, 10]
                )[0]
                
                if missing_components == 'address':
                    address = None
                elif missing_components == 'city':
                    city = None
                elif missing_components == 'state':
                    state = None
                elif missing_components == 'zip':
                    zip_code = None
                elif missing_components == 'multiple':
                    address = city = None
            
            customer = {
                'customer_id': f'CUST{i+1:06d}',
                'first_name': first_name,
                'last_name': last_name,
                'email': email,
                'phone': phone,
                'address': address,
                'city': city,
                'state': state,
                'zip_code': zip_code,
                'date_of_birth': self.fake.date_of_birth(minimum_age=18, maximum_age=80),
                'gender': random.choice(['Male', 'Female', 'Other', None]),  # Allow missing gender
                'registration_date': registration_date,
                'loyalty_member': random.choice([True, False, None]),  # Allow missing loyalty status
                'preferred_contact': random.choice(['Email', 'Phone', 'SMS', None]),
                'customer_segment': random.choice(['Premium', 'Regular', 'Budget', 'VIP']),
                'total_lifetime_value': round(random.uniform(-100, 5000), 2)  # Allow negative LTV
            }
            
            generated_emails.add(email if email else '')
            generated_phones.add(phone if phone else '')
            base_customers.append(customer)
        
        self.customers = base_customers.copy()
        
        # Add extensive duplicate customers (30% duplicate rate)
        if self.add_noise:
            num_duplicates = int(len(base_customers) * self.noise_config['duplicate_customers'])
            for _ in range(num_duplicates):
                original = random.choice(base_customers)
                duplicate = original.copy()
                
                # Create customer ID for duplicate
                duplicate['customer_id'] = f'CUST{len(self.customers)+1:06d}'
                
                # Create realistic variations for duplicates
                variations = [
                    'name_case',      # Different case
                    'name_typo',      # Typos in name
                    'email_variation', # Email variations
                    'phone_format',   # Different phone format
                    'address_format', # Address formatting
                    'partial_info',   # Missing some information
                ]
                
                variation = random.choice(variations)
                
                if variation == 'name_case':
                    if duplicate['first_name']:
                        duplicate['first_name'] = duplicate['first_name'].upper()
                    if duplicate['last_name']:
                        duplicate['last_name'] = duplicate['last_name'].lower()
                
                elif variation == 'name_typo':
                    if duplicate['first_name'] and len(duplicate['first_name']) > 3:
                        # Introduce typo
                        name = list(duplicate['first_name'])
                        pos = random.randint(1, len(name)-2)
                        name[pos] = random.choice(string.ascii_letters)
                        duplicate['first_name'] = ''.join(name)
                
                elif variation == 'email_variation':
                    if duplicate['email'] and '@' in duplicate['email']:
                        base_email = duplicate['email'].split('@')[0]
                        domain = duplicate['email'].split('@')[1]
                        duplicate['email'] = f"{base_email}.{random.randint(1,99)}@{domain}"
                
                elif variation == 'phone_format':
                    if duplicate['phone']:
                        # Different formatting of same number
                        digits = re.sub(r'[^\d]', '', duplicate['phone'])[:10]
                        if len(digits) == 10:
                            duplicate['phone'] = f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
                
                elif variation == 'address_format':
                    if duplicate['address']:
                        duplicate['address'] = duplicate['address'].replace('St', 'Street').replace('Ave', 'Avenue')
                
                elif variation == 'partial_info':
                    # Remove some information to create partial duplicate
                    duplicate['phone'] = None
                    duplicate['address'] = None
                
                self.customers.append(duplicate)
                self.duplicate_customers.append((original['customer_id'], duplicate['customer_id']))
    
    def _weighted_choice(self, choices_dict):
        """Make a weighted random choice from a dictionary."""
        choices = list(choices_dict.keys())
        weights = list(choices_dict.values())
        return random.choices(choices, weights=weights)[0]
    
    def generate_daily_transactions(self, date: datetime) -> List[Dict]:
        """Generate transactions for a specific date with extensive quality issues."""
        transactions = []
        
        # Adjust transaction volume based on day of week
        day_multipliers = {
            0: 1.2,  # Monday
            1: 1.0,  # Tuesday
            2: 1.0,  # Wednesday
            3: 1.1,  # Thursday
            4: 1.3,  # Friday
            5: 1.5,  # Saturday
            6: 1.2   # Sunday
        }
        
        daily_volume = int(self.daily_transactions * day_multipliers[date.weekday()])
        
        for i in range(daily_volume):
            # Select random customer and store
            customer = random.choice(self.customers)
            store = random.choice(self.stores)
            
            # Generate transaction
            transaction = self._generate_single_transaction(date, customer, store, i+1)
            transactions.append(transaction)
        
        # Add duplicate transactions (15% duplicate rate)
        if self.add_noise:
            num_duplicate_transactions = int(len(transactions) * self.noise_config['duplicate_transactions'])
            for _ in range(num_duplicate_transactions):
                original = random.choice(transactions)
                duplicate = original.copy()
                
                # Create slight variations for duplicate transactions
                duplicate['transaction_id'] = f"DUP{duplicate['transaction_id']}"
                
                # Timestamp variation (duplicate might be recorded later)
                if duplicate['datetime']:
                    original_dt = datetime.strptime(duplicate['datetime'], '%Y-%m-%d %H:%M:%S')
                    new_dt = original_dt + timedelta(minutes=random.randint(1, 30))
                    duplicate['datetime'] = new_dt.strftime('%Y-%m-%d %H:%M:%S')
                    duplicate['time'] = new_dt.strftime('%H:%M:%S')
                
                # Sometimes duplicates have different status
                if random.random() < 0.3:
                    duplicate['status'] = 'Failed'
                
                transactions.append(duplicate)
                self.duplicate_transactions.append((original['transaction_id'], duplicate['transaction_id']))
        
        return transactions
    
    def _generate_single_transaction(self, date: datetime, customer: Dict, store: Dict, transaction_num: int) -> Dict:
        """Generate a single transaction with realistic data quality issues."""
        
        # Transaction timing with timestamp issues
        base_transaction_time = date.replace(
            hour=random.randint(8, 22),
            minute=random.randint(0, 59),
            second=random.randint(0, 59)
        )
        
        # Apply timestamp issues
        transaction_time = self._introduce_timestamp_issues(base_transaction_time)
        
        # Handle missing timestamps
        if self.add_noise and random.random() < self.noise_config['missing_transaction_time']:
            transaction_time = None
        
        # Determine transaction status
        status = 'Completed'
        refund_reason = None
        
        if self.add_noise:
            if random.random() < 0.015:  # 1.5% failed transactions
                status = 'Failed'
            elif random.random() < 0.025:  # 2.5% refunds
                status = 'Refunded'
                refund_reason = random.choice([
                    'Customer request', 'Defective product', 'Wrong item', 
                    'Price adjustment', 'Damaged packaging', 'Changed mind'
                ])
        
        # Number of items with potential negative quantities
        num_items = random.choices([1, 2, 3, 4, 5], weights=[40, 30, 20, 7, 3])[0]
        
        # Select products (including out-of-stock or invalid references)
        available_products = self.products.copy()
        
        # Add reference integrity errors (invalid product IDs)
        if self.add_noise and random.random() < self.noise_config['reference_integrity_errors']:
            # Create fake product reference
            fake_product = {
                'product_id': f'INVALID{random.randint(1000, 9999)}',
                'product_name': 'Unknown Product',
                'category': 'Unknown',
                'price': 0.00
            }
            available_products.append(fake_product)
        
        selected_products = random.sample(available_products, min(num_items, len(available_products)))
        
        # Calculate totals with potential errors
        subtotal = 0
        items = []
        
        for product in selected_products:
            # Quantity with potential negative values
            quantity = random.randint(1, 3)
            if self.add_noise and random.random() < self.noise_config['negative_quantities']:
                quantity = random.randint(-5, -1)  # Negative quantity (return/error)
            
            unit_price = product.get('price', 0)
            
            # Handle price data type issues
            if isinstance(unit_price, str):
                try:
                    unit_price = float(unit_price.replace('$', '').replace(',', ''))
                except:
                    unit_price = 0.00
            
            # Apply random discount occasionally
            discount = 0
            if random.random() < 0.15:  # 15% chance of discount
                discount = random.uniform(0.05, 0.25)
            
            discounted_price = unit_price * (1 - discount)
            line_total = discounted_price * quantity
            subtotal += line_total
            
            items.append({
                'product_id': product['product_id'],
                'product_name': self._introduce_encoding_issues(product.get('product_name', 'Unknown')),
                'category': product.get('category', 'Unknown'),
                'quantity': quantity,
                'unit_price': unit_price,
                'discount_percent': round(discount * 100, 2),
                'line_total': round(line_total, 2)
            })
        
        # Calculate tax and total with potential errors
        tax_rate = 0.08  # 8% tax
        tax_amount = subtotal * tax_rate
        total_amount = subtotal + tax_amount
        
        # Add calculation errors
        if self.add_noise and random.random() < 0.05:
            # Tax calculation error
            tax_amount = subtotal * random.uniform(0.05, 0.15)
            total_amount = subtotal + tax_amount
        
        # Cashier ID (potentially missing)
        cashier_id = f'EMP{random.randint(1, 200):03d}'
        if self.add_noise and random.random() < self.noise_config['missing_cashier_id']:
            cashier_id = None
        
        # Payment method (potentially missing)
        payment_method = self._weighted_choice(self.payment_methods)
        if self.add_noise and random.random() < self.noise_config['missing_payment_method']:
            payment_method = None
        
        # Generate transaction
        transaction = {
            'transaction_id': f'TXN{date.strftime("%Y%m%d")}{transaction_num:06d}',
            'date': date.strftime('%Y-%m-%d') if transaction_time else None,
            'time': transaction_time.strftime('%H:%M:%S') if transaction_time else None,
            'datetime': transaction_time.strftime('%Y-%m-%d %H:%M:%S') if transaction_time else None,
            'customer_id': customer['customer_id'],
            'store_id': store['store_id'],
            'store_name': self._introduce_encoding_issues(store.get('store_name', '')),
            'cashier_id': cashier_id,
            'payment_method': payment_method,
            'subtotal': round(subtotal, 2),
            'tax_amount': round(tax_amount, 2),
            'total_amount': round(total_amount, 2),
            'items_count': num_items,
            'items': items,
            'loyalty_points_earned': int(total_amount * 0.1) if customer.get('loyalty_member') else 0,
            'promotion_code': self._generate_promotion_code() if random.random() < 0.1 else None,
            'refund_reason': refund_reason,
            'status': status
        }
        
        # Apply data type inconsistencies to transaction
        # if self.add_noise:
        #     transaction['subtotal'] = self._introduce_data_type_inconsistency(transaction['subtotal'])
        #     transaction['total_amount'] = self._introduce_data_type_inconsistency(transaction['total_amount'])
        #     transaction['items_count'] = self._introduce_data_type_inconsistency(transaction['items_count'])
        
        return transaction
    
    def _generate_promotion_code(self) -> str:
        """Generate random promotion codes."""
        codes = ['SAVE10', 'SUMMER20', 'NEWCUST15', 'LOYALTY5', 'WEEKEND25', 'FLASH30']
        return random.choice(codes)
    
    def save_master_data(self, output_dir='/opt/airflow/master_data'):
        """Save master data (stores, products, customers) to Parquet files."""
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # Save stores
        if self.stores:
            stores_df = pd.DataFrame(self.stores)
            stores_df.to_parquet(f'{output_dir}/stores.parquet', index=False)
        
        # Save products
        if self.products:
            products_df = pd.DataFrame(self.products)
            products_df.to_parquet(f'{output_dir}/products.parquet', index=False)
        
        # Save customers
        if self.customers:
            customers_df = pd.DataFrame(self.customers)
            customers_df.to_parquet(f'{output_dir}/customers.parquet', index=False)
        
        # Save quality issues summary
        quality_report = {
            'duplicate_customers': len(self.duplicate_customers),
            'duplicate_products': len(self.duplicate_products),
            'total_stores': len(self.stores),
            'total_products': len(self.products),
            'total_customers': len(self.customers),
            'noise_config': self.noise_config,
            'generation_timestamp': datetime.now().isoformat()
        }
        
        with open(f'{output_dir}/data_quality_report.json', 'w') as f:
            json.dump(quality_report, f, indent=2)
        
        print(f"Master data saved to {output_dir}/ directory")
        print(f"Generated {len(self.stores)} stores, {len(self.products)} products, {len(self.customers)} customers")
        print(f"Data quality issues: {len(self.duplicate_customers)} duplicate customers, {len(self.duplicate_products)} duplicate products")
    
    def force_regenerate_master_data(self, output_dir='/opt/airflow/master_data'):
        """Force regeneration of master data (useful for testing or updates)."""
        print("Force regenerating master data...")
        self.stores = []
        self.products = []
        self.customers = []
        self.duplicate_customers = []
        self.duplicate_products = []
        
        self._generate_stores()
        self._generate_products()
        self._generate_customers()
        
        self.save_master_data(output_dir)
    
    def generate_and_save_daily_data(self, date: datetime, output_dir='/tmp/retail_data/transactions'):
        """Generate and save transaction data for a specific date."""
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # Check if data for this date already exists
        date_str = date.strftime('%Y-%m-%d')
        transactions_file = f'{output_dir}/transactions_{date_str}.parquet'
        
        if os.path.exists(transactions_file):
            print(f"Transaction data for {date_str} already exists. Skipping generation.")
            print(f"To regenerate, delete {transactions_file} first.")
            return []
        
        transactions = self.generate_daily_transactions(date)
        
        # Save detailed transactions
        # Flatten transaction data for Parquet
        flattened_transactions = []
        for txn in transactions:
            base_txn = {k: v for k, v in txn.items() if k != 'items'}
            if txn['items']:  # Handle empty items list
                for item in txn['items']:
                    row = {**base_txn, **item}
                    flattened_transactions.append(row)
            else:
                # Transaction with no items (data quality issue)
                base_txn.update({
                    'product_id': None,
                    'product_name': None,
                    'category': None,
                    'quantity': 0,
                    'unit_price': 0,
                    'discount_percent': 0,
                    'line_total': 0
                })
                flattened_transactions.append(base_txn)
        
        if flattened_transactions:
            transactions_df = pd.DataFrame(flattened_transactions)
            # Handle datetime conversion with missing values
            transactions_df['datetime'] = pd.to_datetime(transactions_df['datetime'], errors='coerce')
            transactions_df['date'] = pd.to_datetime(transactions_df['date'], errors='coerce')
            transactions_df.to_parquet(transactions_file, index=False)
        
        # Save summary data with quality metrics
        summary_file = f'{output_dir}/daily_summary_{date_str}.json'
        summary = {
            'date': date_str,
            'total_transactions': len(transactions),
            'total_revenue': sum(txn['total_amount'] for txn in transactions if isinstance(txn['total_amount'], (int, float))),
            'total_items_sold': sum(txn['items_count'] for txn in transactions if isinstance(txn['items_count'], (int, float))),
            'unique_customers': len(set(txn['customer_id'] for txn in transactions if txn['customer_id'])),
            'duplicate_transactions': len(self.duplicate_transactions),
            'failed_transactions': len([t for t in transactions if t['status'] == 'Failed']),
            'refunded_transactions': len([t for t in transactions if t['status'] == 'Refunded']),
            'missing_timestamps': len([t for t in transactions if not t['datetime']]),
            'missing_cashier_ids': len([t for t in transactions if not t['cashier_id']]),
            'negative_quantities': len([t for t in transactions for item in t['items'] if item['quantity'] < 0]),
            'payment_method_breakdown': self._get_payment_breakdown(transactions),
            'category_breakdown': self._get_category_breakdown(transactions),
            'top_products': self._get_top_products(transactions)
        }
        
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, default=str)
        
        print(f"Generated {len(transactions)} transactions for {date_str}")
        print(f"Total revenue: ${summary['total_revenue']:,.2f}")
        print(f"Data quality issues: {summary['duplicate_transactions']} duplicates, {summary['failed_transactions']} failed, {summary['missing_timestamps']} missing timestamps")
        print(f"Files saved: {transactions_file}, {summary_file}")
        
        return transactions
    
    def _get_payment_breakdown(self, transactions):
        """Get payment method breakdown."""
        payment_counts = {}
        for txn in transactions:
            method = txn['payment_method'] or 'Unknown'
            payment_counts[method] = payment_counts.get(method, 0) + 1
        return payment_counts
    
    def _get_category_breakdown(self, transactions):
        """Get category sales breakdown."""
        category_sales = {}
        for txn in transactions:
            for item in txn['items']:
                category = item['category'] or 'Unknown'
                if category not in category_sales:
                    category_sales[category] = {'count': 0, 'revenue': 0}
                category_sales[category]['count'] += abs(item['quantity'])  # Use absolute value
                category_sales[category]['revenue'] += item['line_total']
        return category_sales
    
    def _get_top_products(self, transactions, top_n=10):
        """Get top selling products."""
        product_sales = {}
        for txn in transactions:
            for item in txn['items']:
                product_id = item['product_id']
                if product_id not in product_sales:
                    product_sales[product_id] = {
                        'product_name': item['product_name'],
                        'quantity_sold': 0,
                        'revenue': 0
                    }
                product_sales[product_id]['quantity_sold'] += abs(item['quantity'])
                product_sales[product_id]['revenue'] += item['line_total']
        
        # Sort by revenue and return top N
        sorted_products = sorted(product_sales.items(), 
                               key=lambda x: x[1]['revenue'], 
                               reverse=True)
        return dict(sorted_products[:top_n])

def generate_transactions(year, month, date):
    """Main function to demonstrate the enhanced data generator with realistic quality issues."""
    print("Initializing Enhanced Retail Data Generator with Realistic Quality Issues...")
    print("=" * 70)
    
    # Initialize with noise enabled for realistic data quality issues
    generator = RetailDataGenerator(add_noise=True)
    
    # Generate data for specified date
    print(f"\nGenerating transaction data for {year}-{month}-{date}...")
    data_date = datetime(int(year), int(month), int(date))
    generator.generate_and_save_daily_data(data_date)
    
    print("\nData generation complete!")
    print("=" * 70)
    print("\nEnhanced Data Quality Issues Implemented:")
    print("✓ 30% Missing Values:")
    print("  - 35% missing customer emails")
    print("  - 40% missing phone numbers")
    print("  - 25% missing addresses")
    print("  - 30% missing product descriptions")
    print("  - 20% missing cashier IDs")
    
    print("\n✓ 30% Duplicates:")
    print("  - 30% duplicate customers with variations")
    print("  - 25% duplicate products")
    print("  - 15% duplicate transactions")
    print("  - 20% duplicate stores")
    
    print("\n✓ Timestamp Issues:")
    print("  - 25% delayed timestamps (batch processing)")
    print("  - 10% batch processing delays")
    print("  - 8% timezone inconsistencies")
    print("  - 5% system clock drift")
    print("  - 2% future timestamps")
    
    print("\n✓ Additional Real-World Issues:")
    print("  - 12% invalid email formats")
    print("  - 25% inconsistent phone formatting")
    print("  - 8% price inconsistencies")
    print("  - 3% negative quantities")
    print("  - 5% extreme/unrealistic prices")
    print("  - 8% encoding/special character issues")
    print("  - 15% data type inconsistencies")
    print("  - 10% reference integrity errors")
    
    print("\n" + "=" * 70)
    print("Files Generated:")
    print("📁 Master Data (generated once):")
    print("  - stores.parquet (store information)")
    print("  - products.parquet (product catalog)")
    print("  - customers.parquet (customer database)")
    print("  - data_quality_report.json (quality metrics)")
    
    print("\n📁 Daily Data (generated each run):")
    print(f"  - transactions_{data_date.strftime('%Y-%m-%d')}.parquet (daily transactions)")
    print(f"  - daily_summary_{data_date.strftime('%Y-%m-%d')}.json (daily analytics)")
    
    print("\n" + "=" * 70)
    print("Usage Examples:")
    print("import pandas as pd")
    print("import json")
    print("")
    print("# Load transaction data")
    print(f"df = pd.read_parquet('/tmp/retail_data/transactions/transactions_{data_date.strftime('%Y-%m-%d')}.parquet')")
    print("")
    print("# Load quality report")
    print("with open('/tmp/retail_data/transactions/data_quality_report.json') as f:")
    print("    quality_report = json.load(f)")
    print("")
    print("# Check for duplicates")
    print("duplicates = df[df.duplicated(['customer_id', 'datetime'], keep=False)]")
    print("")
    print("# Check for missing values")
    print("missing_data = df.isnull().sum()")
    print("")
    print("# Analyze timestamp issues")
    print("timestamp_issues = df[df['datetime'].isnull()]")

if __name__ == "__main__":
    generate_transactions(sys.argv[1], sys.argv[2], sys.argv[3])