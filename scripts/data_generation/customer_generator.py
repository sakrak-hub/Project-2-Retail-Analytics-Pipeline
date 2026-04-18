import random
import re
import string
from base_generator import BaseGenerator


class CustomerGenerator(BaseGenerator):
    """Generates customer database data with optional noise and duplicates."""

    def __init__(self, num_customers=55000, **kwargs):
        super().__init__(**kwargs)
        self.num_customers = num_customers

    def generate(self) -> tuple:
        """Generate and return (customers list, duplicate_pairs list)."""
        generated_emails = set()
        generated_phones = set()
        base_customers = []
        duplicate_customers = []

        for i in range(self.num_customers):
            registration_date = self.fake.date_between(start_date='-3y', end_date='today')

            first_name = self.fake.first_name()
            last_name = self.fake.last_name()
            email = self.fake.email()
            phone = self.fake.phone_number()

            if self.add_noise:
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
                        lambda e: e[:-4] if e.endswith('.com') else e,
                    ]
                    email = random.choice(email_issues)(email)

                # Inconsistent phone formats
                if phone and random.random() < self.noise_config['inconsistent_phone_format']:
                    phone_formats = [
                        lambda p: re.sub(r'[^\d]', '', p)[:10],
                        lambda p: f'+1-{p}',
                        lambda p: p.replace('-', '.').replace('(', '').replace(')', ''),
                        lambda p: p + 'x123',
                        lambda p: p[:3] + p[6:] if len(p) > 6 else p,
                    ]
                    phone = random.choice(phone_formats)(phone)

                first_name = self._introduce_encoding_issues(first_name)
                last_name = self._introduce_encoding_issues(last_name)

            address = self.fake.address()
            city = self.fake.city()
            state = self.fake.state()
            zip_code = self.fake.zipcode()

            if self.add_noise and random.random() < self.noise_config['missing_customer_address']:
                missing_components = random.choices(
                    ['address', 'city', 'state', 'zip', 'multiple'],
                    weights=[30, 20, 20, 20, 10],
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
                'gender': random.choice(['Male', 'Female', 'Other', None]),
                'registration_date': registration_date,
                'loyalty_member': random.choice([True, False, None]),
                'preferred_contact': random.choice(['Email', 'Phone', 'SMS', None]),
                'customer_segment': random.choice(['Premium', 'Regular', 'Budget', 'VIP']),
                'total_lifetime_value': round(random.uniform(-100, 5000), 2),
            }

            generated_emails.add(email if email else '')
            generated_phones.add(phone if phone else '')
            base_customers.append(customer)

        customers = base_customers.copy()

        # Add duplicate customers (30% duplicate rate)
        if self.add_noise:
            num_duplicates = int(len(base_customers) * self.noise_config['duplicate_customers'])
            for _ in range(num_duplicates):
                original = random.choice(base_customers)
                duplicate = original.copy()

                duplicate['customer_id'] = f'CUST{len(customers)+1:06d}'

                variations = [
                    'name_case', 'name_typo', 'email_variation',
                    'phone_format', 'address_format', 'partial_info',
                ]
                variation = random.choice(variations)

                if variation == 'name_case':
                    if duplicate['first_name']:
                        duplicate['first_name'] = duplicate['first_name'].upper()
                    if duplicate['last_name']:
                        duplicate['last_name'] = duplicate['last_name'].lower()

                elif variation == 'name_typo':
                    if duplicate['first_name'] and len(duplicate['first_name']) > 3:
                        name = list(duplicate['first_name'])
                        pos = random.randint(1, len(name) - 2)
                        name[pos] = random.choice(string.ascii_letters)
                        duplicate['first_name'] = ''.join(name)

                elif variation == 'email_variation':
                    if duplicate['email'] and '@' in duplicate['email']:
                        base_email = duplicate['email'].split('@')[0]
                        domain = duplicate['email'].split('@')[1]
                        duplicate['email'] = f"{base_email}.{random.randint(1,99)}@{domain}"

                elif variation == 'phone_format':
                    if duplicate['phone']:
                        digits = re.sub(r'[^\d]', '', duplicate['phone'])[:10]
                        if len(digits) == 10:
                            duplicate['phone'] = f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"

                elif variation == 'address_format':
                    if duplicate['address']:
                        duplicate['address'] = duplicate['address'].replace('St', 'Street').replace('Ave', 'Avenue')

                elif variation == 'partial_info':
                    duplicate['phone'] = None
                    duplicate['address'] = None

                customers.append(duplicate)
                duplicate_customers.append((original['customer_id'], duplicate['customer_id']))

        return customers, duplicate_customers
