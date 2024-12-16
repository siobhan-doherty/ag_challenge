import pandas as pd
from faker import Faker

fake = Faker()


def generate_users(num_users):
    users = []
    for i in range(1, num_users + 1):
        users.append({
            "id": i,
            "name": fake.name(),
            "email": fake.email(),
            "registration_date": fake.date_this_decade()
        })

    return pd.DataFrame(users)


if __name__ == "__main__":
    users_df = generate_users(10)
    users_df.to_csv("data/structure/users_data.csv", index=False)
    print("Users data generated successfully!")
