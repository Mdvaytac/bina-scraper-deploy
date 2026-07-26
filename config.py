from dotenv import load_dotenv
import os

load_dotenv()

BASE_URL = os.getenv("BASE_URL")

PAGE_SIZE = int(os.getenv("PAGE_SIZE"))

CITY_ID = os.getenv("CITY_ID")

CATEGORY_ID = os.getenv("CATEGORY_ID")

LEASED = os.getenv("LEASED") == "true"

SORT = os.getenv("SORT")

REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT"))