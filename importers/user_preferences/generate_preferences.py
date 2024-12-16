import random
import pandas as pd
from faker import Faker

fake = Faker()


def generate_preferences(num_users):
    preferences = []
    for i in range(1, num_users + 1):
        preferences.append({
            "id": i,
            "user_id": i,
            "preferred_language": random.choice(["English", "French", "German", "Spanish"]),
            "notifications_enabled": random.choice([True, False]),
            "marketing_opt_in": random.choice([True, False]),
            "created_at": fake.date_time_this_year(),
            "updated_at": fake.date_time_this_year()
        })
    
    return pd.DataFrame(preferences)


if __name__ == "__main__":
    preferences_df = generate_preferences(10)
    preferences_df.to_csv("data/structure/user_preferences_data.csv", index=False)
    print("User preferences data generated successfully!")
