import subprocess


def run_scripts():
    scripts = [
        "python importers/transactions/generate_transactions.py",
        "python importers/user_preferences/generate_preferences.py",
        "python importers/users/generate_users.py"
    ]
    for script in scripts:
        subprocess.run(script, shell=True)


if __name__ == "__main__":
    run_scripts()
    print("All data generated successfully!")
