import random
from base_generator import BaseGenerator


class ProductGenerator(BaseGenerator):
    """Generates product catalog data with optional noise and duplicates."""

    def __init__(self, num_products=12000, **kwargs):
        super().__init__(**kwargs)
        self.num_products = num_products

    def generate(self) -> tuple:
        """Generate and return (products list, duplicate_pairs list)."""
        brands = [self.fake.company() for _ in range(200)]
        generated_skus = set()
        base_products = []
        duplicate_products = []

        for i in range(self.num_products):
            category = random.choice(list(self.product_categories.keys()))
            price_range = self.product_categories[category]
            base_price = random.uniform(price_range[0], price_range[1])

            # SKU with potential issues
            sku = f'SKU{random.randint(100000, 999999)}'
            if self.add_noise and random.random() < self.noise_config['missing_product_sku']:
                sku = None
            elif sku in generated_skus and random.random() < 0.15:
                sku = f'{sku}-{random.randint(1, 99)}'

            if sku:
                generated_skus.add(sku)

            # Description with missing data
            description = self.fake.text(max_nb_chars=200)
            if self.add_noise and random.random() < self.noise_config['missing_product_description']:
                description = None

            # Price with quality issues
            cost = round(base_price * random.uniform(0.4, 0.7), 2)
            if self.add_noise:
                if random.random() < self.noise_config['price_inconsistency']:
                    cost = round(base_price * random.uniform(1.1, 1.5), 2)
                # if random.random() < self.noise_config['extreme_prices']:
                #     base_price = random.choice([
                #         round(random.uniform(0.01, 0.1), 2),
                #         round(random.uniform(50000, 100000), 2),
                #     ])

            weight = round(random.uniform(0.1, 50), 2)
            dimensions = f'{random.randint(1,50)}x{random.randint(1,50)}x{random.randint(1,50)}'

            if self.add_noise and random.random() < 0.15:
                weight = (
                    round(random.uniform(0.001, 0.01), 3)
                    if random.choice([True, False])
                    else round(random.uniform(500, 1000), 2)
                )

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
                'stock_quantity': random.randint(-10, 1000),
                'supplier': self.fake.company(),
                'launch_date': self.fake.date_between(start_date='-2y', end_date='today'),
            }

            if self.add_noise:
                product['product_name'] = self._introduce_encoding_issues(product['product_name'])
                product['brand'] = self._introduce_encoding_issues(product['brand'])
                if product['description']:
                    product['description'] = self._introduce_encoding_issues(product['description'])

            base_products.append(product)

        products = base_products.copy()

        # Add duplicate products with variations
        if self.add_noise:
            num_duplicates = int(len(base_products) * self.noise_config['duplicate_products'])
            for _ in range(num_duplicates):
                original = random.choice(base_products)
                duplicate = original.copy()

                duplicate['product_id'] = f'PRD{len(products)+1:06d}'
                duplicate['sku'] = f"{duplicate['sku']}-DUP" if duplicate['sku'] else None

                if isinstance(duplicate['price'], (int, float)):
                    duplicate['price'] = round(duplicate['price'] * random.uniform(0.95, 1.05), 2)

                if duplicate['description']:
                    duplicate['description'] = duplicate['description'][:100] + '...'

                products.append(duplicate)
                duplicate_products.append((original['product_id'], duplicate['product_id']))

        return products, duplicate_products

    def _generate_product_name(self, category) -> str:
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
            'Pet Supplies': ['Dog Food', 'Cat Toy', 'Pet Bed', 'Leash', 'Pet Carrier'],
        }

        items = category_items.get(category, ['Product'])
        return f'{random.choice(adjectives)} {random.choice(items)}'

    def _generate_subcategory(self, category) -> str:
        """Generate subcategory for a given main category."""
        subcategories = {
            'Electronics': ['Mobile Phones', 'Computers', 'Audio', 'Gaming', 'Accessories'],
            'Clothing': ["Men's Wear", "Women's Wear", "Children's Wear", 'Footwear', 'Accessories'],
            'Home & Garden': ['Furniture', 'Decor', 'Kitchen', 'Garden', 'Storage'],
            'Books': ['Fiction', 'Non-Fiction', 'Educational', "Children's Books", 'Reference'],
            'Sports & Outdoors': ['Fitness', 'Outdoor Recreation', 'Team Sports', 'Water Sports'],
            'Beauty & Personal Care': ['Skincare', 'Haircare', 'Makeup', 'Fragrance', 'Personal Hygiene'],
            'Food & Beverages': ['Beverages', 'Snacks', 'Organic', 'International', 'Gourmet'],
            'Toys & Games': ['Educational', 'Action Figures', 'Board Games', 'Electronic Toys'],
            'Automotive': ['Parts', 'Accessories', 'Maintenance', 'Electronics', 'Tools'],
            'Health & Wellness': ['Supplements', 'Medical Devices', 'Fitness', 'Personal Care'],
            'Office Supplies': ['Stationery', 'Technology', 'Furniture', 'Organization', 'Art Supplies'],
            'Pet Supplies': ['Food', 'Toys', 'Accessories', 'Health', 'Grooming'],
        }
        return random.choice(subcategories.get(category, ['General']))
