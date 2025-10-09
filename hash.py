# When you do:
my_dict = {"user_id": 123, "name": "John"}

# Python automatically:
# 1. Calculates hash("user_id") → e.g., 183492834
# 2. Uses this hash to find memory location
# 3. Direct O(1) access

print(hash("user_id"))  # You can see the hash value
