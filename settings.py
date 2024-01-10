import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

DATABASE_HOST = os.getenv('DATABASE_HOST', default=None)
DATABASE_USER = os.getenv('DATABASE_USER', default=None)
DATABASE_PASSWORD = os.getenv('DATABASE_PASSWORD', default=None)
DATABASE_NAME = os.getenv('DATABASE_NAME', default=None)
RED_SHIFT_HOST = os.getenv('RED_SHIFT_HOST', default=None)
RED_SHIFT_PORT = os.getenv('RED_SHIFT_PORT', default=None)
RED_SHIFT_DBNAME = os.getenv('RED_SHIFT_DBNAME', default=None)
RED_SHIFT_USER = os.getenv('RED_SHIFT_USER', default=None)
RED_SHIFT_PASSWORD = os.getenv('RED_SHIFT_PASSWORD', default=None)
