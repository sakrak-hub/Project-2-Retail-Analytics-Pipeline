import random
from base_generator import BaseGenerator

class StoreGenerator(BaseGenerator):
    """Generates store location data with optional noise and duplicates."""

    def generate(self) -> list:
        """Generate and return a list of store dictionaries."""
        store_types = ['Flagship', 'Mall', 'Outlet', 'Express', 'Online']
        base_stores = []

        for i in range(10):
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
                'opening_date': self.fake.date_between(start_date='-5y', end_date='today'),
            }

            if self.add_noise:
                if random.random() < 0.25:
                    store['phone'] = None
                if random.random() < 0.15:
                    store['manager'] = None
                if random.random() < 0.10:
                    store['address'] = None

                store['city'] = self._introduce_encoding_issues(store['city'])

            base_stores.append(store)

        stores = base_stores.copy()

        # Add duplicate stores with variations
        if self.add_noise:
            num_duplicates = int(len(base_stores) * self.noise_config['duplicate_stores'])
            for _ in range(num_duplicates):
                original = random.choice(base_stores)
                duplicate = original.copy()

                duplicate['store_id'] = f'ST{len(stores)+1:03d}'
                duplicate['store_name'] = duplicate['store_name'] + ' Branch'

                if duplicate['address']:
                    duplicate['address'] = duplicate['address'].replace('St', 'Street')

                stores.append(duplicate)

        return stores
