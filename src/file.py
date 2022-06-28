import os
if os.path.exists("json/file.json"):
    print("exists!")
else:
    print("doesn't exist")
    with open("json/file.json", "w") as f:
        print("created")
