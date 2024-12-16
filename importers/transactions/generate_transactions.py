import random
import pandas as pd
from faker import Faker

fake = Faker()


def generate_transactions(num_transactions, num_users):
    transactions = []
    for i in range(1, num_transactions + 1):
        transactions.append({
            "id": i, 
            "user_id": random.randint(1, num_users),
            "transaction_date": fake.date_this_year(),
            "amount": round(random.uniform(10, 500), 2),
            "type": random.choice(["deposit", "withdrawal"])
        })
    return pd.DataFrame(transactions)


if __name__ == "__main__":
    users_number = 10
    transactions_number = 30
    transactions_df = generate_transactions(transactions_number, users_number)
    transactions_df.to_csv("data/structure/transactions_data.csv", index=False)
    print("Transactions data generated successfully!")
